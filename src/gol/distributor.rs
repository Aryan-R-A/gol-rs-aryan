use crate::gol::Params;
use crate::gol::event::{Event, State};
use crate::gol::io::IoCommand;
use crate::util::cell::{CellValue, CellCoord};
use anyhow::Result;
use flume::{Receiver, Sender};
use sdl2::keyboard::Keycode;

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
        for y in 0..height {
            for x in 0..width {
                // Convert 2D coordinates (x, y) into a 1D index.
                let idx = y * width + x;

                // Count how many alive neighbours surround this cell.
                let alive_neighbours = count_neighbours(&current, x, y, width, height);

                let cell = current[idx];

                // a live cell with 2 or 3 live neighbours survives.
                // a dead cell with exactly 3 live neighbours becomes alive.
                // all other cells become/stay dead.
                next[idx] = match (cell, alive_neighbours) {
                    (CellValue::Alive, 2) | (_, 3) => CellValue::Alive,
                    _ => CellValue::Dead,
                };
            }
        }

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

