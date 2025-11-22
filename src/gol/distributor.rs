use crate::gol::Params;
use crate::gol::event::{Event, State};
use crate::gol::io::IoCommand;
use crate::util::cell::{CellValue, CellCoord};
use anyhow::Result;
use flume::{Receiver, Sender};
use sdl2::keyboard::Keycode;
use std::thread;
use std::sync::{Arc, atomic::{AtomicU32, Ordering}};
use std::time::Duration;
use rayon::prelude::*;

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

// shared stats used by the AliveCellsCount ticker thread.
pub struct AliveStats {
    turns: AtomicU32, // completed turns so far.
    cells: AtomicU32, // alive cells in the latest completed world.
}

pub fn distributor(params: Params, channels: DistributorChannels) -> Result<()> {
    let events = channels.events.as_ref().unwrap();
    let key_presses = channels.key_presses.as_ref().unwrap();
    let io_command = channels.io_command.as_ref().unwrap();
    let io_idle = channels.io_idle.as_ref().unwrap();
    let io_filename = channels.io_filename.as_ref().unwrap();
    let io_input = channels.io_input.as_ref().unwrap();
    let io_output   = channels.io_output.as_ref().unwrap();


    // shared state for the AliveCellsCount ticker 
    // start with 0 turns and 0 alive cells.
    let stats = Arc::new(AliveStats {
        turns: AtomicU32::new(0),
        cells: AtomicU32::new(0),
    });

    // clone the Arc and the events sender for the ticker thread
    let stats_for_ticker = Arc::clone(&stats);
    let events_for_ticker = events.clone();

    // spawn a dedicated thread that wakes up every 2 seconds and
    // sends an AliveCellsCount event with the latest stats.
    thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_secs(2));      // sleep for 2 seconds between reports

            // read the most recent values written by the main loop
            let current_turns = stats_for_ticker.turns.load(Ordering::Relaxed);
            let current_cells = stats_for_ticker.cells.load(Ordering::Relaxed);

            // send AliveCellsCount.

            if events_for_ticker
                .send(Event::AliveCellsCount {
                    completed_turns: current_turns,
                    cells_count: current_cells,
                })
                .is_err()            // If the receiver has been dropped, send() will return Err and we exit the thread
            {
                break;
            }
        }
    });

    // Clone the Arc and the events sender for the ticker thread.
    let stats_for_ticker = Arc::clone(&stats);
    let events_for_ticker = events.clone();

    // Spawn a dedicated thread that wakes up every 2 seconds and
    // sends an AliveCellsCount event with the latest stats.
    thread::spawn(move || {
        loop {
            // Sleep for 2 seconds between reports.
            thread::sleep(Duration::from_secs(2));

            // Read the most recent values written by the main loop.
            let current_turns = stats_for_ticker.turns.load(Ordering::Relaxed);
            let current_cells = stats_for_ticker.cells.load(Ordering::Relaxed);

            // Try to send AliveCellsCount.
            // If the receiver has been dropped (e.g. distributor finished),
            // send() will return Err and we exit the thread.
            if events_for_ticker
                .send(Event::AliveCellsCount {
                    completed_turns: current_turns,
                    cells_count: current_cells,
                })
                .is_err()
            {
                break;
            }
        }
    });

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
    let input_name = format!("{}x{}", width, height);
    io_filename.send(input_name)?;

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

/* FOR NO WORKER POOL IMPLEMENTATION WITH THREADS AND NOT RAYON

    //  read the current state from current & compute the next state into next.
    //  swap current and next so current always holds the latest world.
    for _ in 0..params.turns {
        update_world_parallel(
            &current,
            &mut next,
            width,
            height,
            params.threads,
        );*/




    // build a fixed-size rayon thread pool .
    // Rayon will create worker threads and reuse them
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(params.threads.max(1))
        .build()
        .expect("failed to build rayon thread pool");

    for _ in 0..params.turns {
        // run game inside the pool.
        // install executes using the pools worker threads.
        pool.install(|| {update_world_parallel_rayon(&current, &mut next, width, height);});

        // Swap current and next
        std::mem::swap(&mut current, &mut next);

        // one turn completed
        completed_turns += 1;

        // update shared stats for the ticker thread &
        // count how many cells are alive in the current world.
        let mut alive_count: u32 = 0;
        for cell in current.iter() {
            if *cell == CellValue::Alive {
                alive_count += 1;
            }
        }

        // store the latest completed turn and alive cell count
        stats.turns.store(completed_turns, Ordering::Relaxed);
        stats.cells.store(alive_count, Ordering::Relaxed);

        //notify test that turn is completed
        events.send(Event::TurnComplete {
            completed_turns,
        })?;
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

    // IO will now save this to "out/<width>x<height>.pgm"
    let output_name = format!("{}x{}x{}", width, height, params.turns); 

    // Tell the IO command we want to output an image.
    io_command.send(IoCommand::IoOutput)?;

    // send the filename to IO so it knows what to call the file
    io_filename.send(output_name.clone())?;

    // send each cell's buffer to IO
    // IO's write_pgm_image() will read exactly width*height CellValue values
    // from the output channel and write them into the file.
    for cell in current.iter() {
        io_output.send(*cell)?;
    }

    // notify that image output has completed
    events.send(Event::ImageOutputComplete {
        completed_turns,
        filename: output_name,
    })?;

    // verify Io has finished any output before exiting
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

/* FOR NO WORKER POOL IMPLEMENTATION WITH THREADS AND NOT RAYON

helper func with helper threads
- current is the read-only world for this turn.
- next is the write-only buffer where we store the new state.
- width & height define the grid dimensions.
- threads is the number of worker threads to use.
the rows [0, height) will be equal-sized chunks,
and each worker is responsible a chunk 
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

    // use a thread::scope so that child threads can borrow current and next from stack
    thread::scope(|scope| {
        // next broken into chunks that dont overlap
        let next_chunks: Vec<&mut [CellValue]> = next.chunks_mut(width * rows_per_thread).collect();

        //one chunk per thread 
        for (t, chunk) in next_chunks.into_iter().enumerate() {
            //compute the global row range [start_row, end_row) for this chunk.
            let start_row = t * rows_per_thread;
            if start_row >= height {
                break; //break if no more rows remain
            }
            let end_row = ((t + 1) * rows_per_thread).min(height);

            let current_world = current; //read only

            // spawn the worker thread for this chunk of rows
            scope.spawn(move || {
                // chunk only has start_row..end_row , we loop using global index y
                //but when we're writing into the chunk we convert it into local row index
                for y in start_row..end_row {
                    for x in 0..width {
                        let idx = y * width + x;    // globval index into the flat current_world

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
    }); }*/



// current is the read-only world for this turn.
// next is the buffer we write the next generation into.
// width and height define the 2D grid, but both current and next are stored as flat 1D arrays of length width * height.
fn update_world_parallel_rayon(
    current: &[CellValue],
    next: &mut [CellValue],
    width: usize,
    height: usize,
) {
    

    next.par_chunks_mut(width) // splits next into mutable chunks (rows) of length width; each row can be processed independently in parallel by different worker threads.
        .enumerate()
        .for_each(|(y, row)| {
            // y here is the row index (0..height),
            // row is &mut [CellValue] for that row in next

            let y = y; // just to make the type clear: usize

            for x in 0..width {
                // global index into the flat current buffer.
                let idx = y * width + x;

                // count neighbours in the full current world.
                let alive_neighbours =
                    count_neighbours(current, x, y, width, height);

                let cell = current[idx];

                let new_value = match (cell, alive_neighbours) {
                    (CellValue::Alive, 2) | (_, 3) => CellValue::Alive,
                    _ => CellValue::Dead,
                };

                // write result into the correct column of this row in next
                row[x] = new_value;
            }
        });
}
