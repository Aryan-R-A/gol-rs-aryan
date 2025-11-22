use crate::gol::Params;
use crate::gol::event::{Event, State};
use crate::gol::io::IoCommand;
use crate::util::cell::{CellValue, CellCoord};
use anyhow::Result;
use flume::{Receiver, Sender};
use sdl2::keyboard::Keycode;
use std::thread;

#[derive(Debug, Clone)]
pub struct DistributorChannels {
    pub events: Option<Sender<Event>>,
    pub key_presses: Option<Receiver<Keycode>>,
    pub io_command: Option<Sender<IoCommand>>,
    pub io_idle: Option<Receiver<bool>>,
    pub io_filename: Option<Sender<String>>,
    pub io_input: Option<Receiver<CellValue>>,
    pub io_output: Option<Sender<CellValue>>,
}

pub fn distributor(params: Params, channels: DistributorChannels) -> Result<()> {
    let events = channels.events.as_ref().unwrap();
    let key_presses = channels.key_presses.as_ref().unwrap();
    let io_command = channels.io_command.as_ref().unwrap();
    let io_idle = channels.io_idle.as_ref().unwrap();
    let io_filename = channels.io_filename.as_ref().unwrap();
    let io_input = channels.io_input.as_ref().unwrap();

    // get the dimensions of the world.
    let width = params.image_width;
    let height = params.image_height;

    // allocate two 1D vectors, one for current gen and one for next gen
    // we treat index y * width + x as the cell at coordinates (x, y).
    let mut current = vec![CellValue::Dead; width * height];
    let mut next = vec![CellValue::Dead; width * height];

    // completed turns tracked & report this in events.
    //u32 to match the type used in the Event definitions.
    let mut completed_turns: u32 = 0;

    // tell IO we want to read image.
    io_command.send(IoCommand::IoInput)?;

    // send the image name to IO.
    let fname = format!("{}x{}", width, height);
    io_filename.send(fname)?;

    // fill current buffer with CellValues received from IO.
    // read_pgm_image in io.rs will send exactly width*height cells.
    for cell in current.iter_mut() {
        *cell = io_input.recv()?; // blocking receive from IO
    }

    let turn = 0;
    events.send(Event::StateChange {
        completed_turns: turn,
        new_state: State::Executing,
    })?;


    //  read the current state from current & compute the next state into next.
    //  swap current and next so current always holds the latest world.
    for _ in 0..params.turns {
        update_world_parallel(
            &current,
            &mut next,
            width,
            height,
            params.threads,
        );

        // Swap current and next
        std::mem::swap(&mut current, &mut next);

        // one turn completed
        completed_turns += 1;
    }


    // build a list of all alive cells in the final world.
    // we scan over every coordinate (x, y), and whenever we see an
    // alive cell in current, we push a corresponding CellCoord into
    // the alive_cells vector.
    let mut alive_cells: Vec<CellCoord<usize>> = Vec::new();

    for y in 0..height {
        for x in 0..width {
            let idx = y * width + x;
            if current[idx] == CellValue::Alive {
                // CellCoord stores the coordinates of an alive cell.
                alive_cells.push(CellCoord { x, y });
            }
        }
    }

    // Send a FinalTurnComplete event so the tests / GUI can inspect
    // the final world state after completed_turns generations.
    events.send(Event::FinalTurnComplete {
        completed_turns,
        alive: alive_cells,
    })?;


    // Make sure that the Io has finished any output before exiting.
    io_command.send(IoCommand::IoCheckIdle)?;
    io_idle.recv()?;

    events.send(Event::StateChange {
        completed_turns: turn,
        new_state: State::Quitting,
    })?;
    Ok(())
}

// helper func that Coucountsnt the number of alive neighbours around the cell at (x, y).
// the world is stored as a flat slice world of length width * height and has grid wrapping
fn count_neighbours(
    world: &[CellValue],
    x: usize,
    y: usize,
    width: usize,
    height: usize,
) -> usize {
    let mut count = 0;

    // Look at the 8 neighbours: (x + dx, y + dy)
    // where dx, dy are in {-1, 0, 1} but not both zero.
    for dy in [-1isize, 0, 1] {
        for dx in [-1isize, 0, 1] {
            if dx == 0 && dy == 0 { // Skip the centre cell itself.
                continue;
            }

            // compute neighbour coordinates with wrap-around.
            // change x/y to isize so we can add the offsets with signs.
            // dadd dx, dy and width/height to keep result non-negative.
            // use rem_euclid to wrap into the valid range [0, width/height).
            //cast back to usize for indexing.
            let nx =(x as isize + dx + width as isize).rem_euclid(width as isize) as usize;
            let ny =(y as isize + dy + height as isize).rem_euclid(height as isize) as usize;

            // convert (nx, ny) back into a 1D index.
            let n_idx = ny * width + nx;

            // If this neighbour cell is alive, increment the count.
            if world[n_idx] == CellValue::Alive {
                count += 1;
            }
        }
    }

    count
}

/*helper func with helper threads
- current is the read-only world for this turn.
- next is the write-only buffer where we store the new state.
- width & height define the grid dimensions.
- threads is the number of worker threads to use.
the rows [0, height) will be equal-sized chunks,
and each worker is responsible a chunk */
fn update_world_parallel(
    current: &[CellValue],
    next: &mut [CellValue],
    width: usize,
    height: usize,
    threads: usize,
) {
    use std::thread;
    // at least 1 thread and not more threads than rows.
    let threads = threads.max(1).min(height);

    let rows_per_thread = (height + threads - 1) / threads; // ceiling division

    // use a thread::scope so that child threads can borrow current and next safely without needing Arc/Mutex
    thread::scope(|scope| {
        // next broken into chunks
        let next_chunks: Vec<&mut [CellValue]> = next.chunks_mut(width * rows_per_thread).collect();

        for (t, chunk) in next_chunks.into_iter().enumerate() {
            let start_row = t * rows_per_thread;
            if start_row >= height {
                break;
            }
            let end_row = ((t + 1) * rows_per_thread).min(height);

            let current_world = current;

            // spawn the worker thread on the working section.
            scope.spawn(move || {
                // local y index goes from start_row..end_row, but for next_slice
                // we global index into current but a local index into next_slice
                for y in start_row..end_row {
                    for x in 0..width {
                        let idx = y * width + x;

                        // count neighbours in current
                        let alive_neighbours = count_neighbours(current_world, x, y, width, height);

                        let cell = current_world[idx];

                        let new_value = match (cell, alive_neighbours) {
                            (CellValue::Alive, 2) | (_, 3) => CellValue::Alive,
                            _ => CellValue::Dead,
                        };

                        // write to correct location in next_slice
                        let local_y = y - start_row;
                        let local_idx = local_y * width + x;

                        chunk[local_idx] = new_value;
                    }
                }
            });
        }
    });
}
