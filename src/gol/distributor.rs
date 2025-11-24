use crate::gol::Params;
use crate::gol::event::{Event, State};
use crate::gol::io::IoCommand;
use crate::util::cell::{CellValue, CellCoord};
use anyhow::Result;
use flume::{Receiver, Sender};
use sdl2::keyboard::Keycode;
use std::thread;
use std::sync::{Arc, Mutex};
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
    pub turns: u32,
    pub cells: u32,
}



pub fn distributor(params: Params, channels: DistributorChannels) -> Result<()> {
    let events = channels.events.as_ref().unwrap();
    let key_presses = channels.key_presses.as_ref().unwrap();
    let io_command = channels.io_command.as_ref().unwrap();
    let io_idle = channels.io_idle.as_ref().unwrap();
    let io_filename = channels.io_filename.as_ref().unwrap();
    let io_input = channels.io_input.as_ref().unwrap();
    let io_output   = channels.io_output.as_ref().unwrap();

    // get the dimensions of the world.
    let width = params.image_width;
    let height = params.image_height;

    // allocate two 1D vectors buffers, one for current gen and one for next gen
    let mut current = vec![CellValue::Dead; width * height];
    let mut next = vec![CellValue::Dead; width * height];

    // completed turns tracked; u32 to match Event types
    let mut completed_turns: u32 = 0;
    let total_turns: u32 = params.turns as u32;

    // tell IO we want to read image.
    io_command.send(IoCommand::IoInput)?;

    // send the image name to IO.
    let input_name = format!("{}x{}", width, height);
    io_filename.send(input_name)?;

    // fill current buffer with CellValues received from IO
    // read_pgm_image in io.rs will send exactly width*height cells
    for cell in current.iter_mut() {
        *cell = io_input.recv()?; // blocking receive from IO
    }

    // shared state for the AliveCellsCount ticker 
    // start with 0 turns and 0 alive cells.
    // start with 0 turns and 0 alive cells.
    let stats = Arc::new(Mutex::new(AliveStats {
        turns: 0,
        cells: 0,
    }));


    {
        // clone the Arc and the events sender for the ticker thread
        let stats_for_ticker = Arc::clone(&stats);
        let events_for_ticker = events.clone();

        // spawn a dedicated thread that wakes up every 2 seconds and
        // sends an AliveCellsCount event with the latest stats.
        thread::spawn(move || {
            loop {
                // sleep for 2 seconds between reports
                thread::sleep(Duration::from_secs(2));

                // read the most recent values written by the main loop
                let (turns, cells) = {
                    let s = stats_for_ticker.lock().unwrap();
                    (s.turns, s.cells)
                };


                // try to send AliveCellsCount
                // if the receiver has been dropped (e.g. distributor finished),
                // send() will return Err and we exit the thread.
                if events_for_ticker
                    .send(Event::AliveCellsCount {
                        completed_turns: turns,
                        cells_count: cells,
                    })
                    .is_err()
                {
                    break;// receiver was dropped; stop the ticker.
                }
            }
        });
    }

    // tell SDL about the initial alive cells (turn 0)
    let mut initial_cells: Vec<CellCoord<usize>> = Vec::new();
    for y in 0..height {
        for x in 0..width {
            let idx = y * width + x;
            if current[idx] == CellValue::Alive {
                initial_cells.push(CellCoord { x, y });
            }
        }
    }

    if !initial_cells.is_empty() 
    {
        // this is the initial state
        events.send(Event::CellsFlipped {completed_turns: 0,cells: initial_cells})?;
    }

    //start the execute
    events.send(Event::StateChange {
        completed_turns: 0,
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
        );

*/

    // execution state for key press: pause/resume/quit.
    let mut paused = false;
    let mut quitting = false;

    // run until we either complete all turns or the user presses 'q'.
    while completed_turns < total_turns {
        // non-blocking key processing
        while let Ok(key) = key_presses.try_recv() {
            match key {
                // toggle pause
                Keycode::P => {
                    paused = !paused;
                    let new_state = if paused { State::Pause } else { State::Executing };
                    events.send(Event::StateChange {
                        completed_turns,
                        new_state,
                    })?;
                }

                // quit request
                Keycode::Q => {
                // just mark that we should quit
                // the final output & events are handled after the loop
                quitting = true;
                }

                // single step (snaoshot of workld)
                Keycode::S => {
                        let output_name = format!("{}x{}x{}", width, height, completed_turns);

                        io_command.send(IoCommand::IoOutput)?;
                        io_filename.send(output_name.clone())?;

                        for cell in current.iter() {
                            io_output.send(*cell)?;
                        }

                        events.send(Event::ImageOutputComplete {
                            completed_turns,
                            filename: output_name,
                        })?;

                        // s must block for idle for test timing to work
                        // aka wait for IO to finish so the test can see the file
                        io_command.send(IoCommand::IoCheckIdle)?;
                        io_idle.recv()?;
                    }
                

                _ => { /*ignore rest */ }
            }
        }

        if paused && quitting {
            break;
        }

        // light sleep so we’re not busy while paused.
        if paused {thread::sleep(Duration::from_millis(5));continue;}

        // normal turn 
        do_one_turn(
            &current,
            &mut next,
            width,
            height,
            &events,
            &stats,
            &mut completed_turns,
        )?;
        std::mem::swap(&mut current, &mut next);
        
        // if Q was pressed during this turn, we’re done after this turn.
        if quitting {
            break;
        }
    
    }

    // after the loop => send final world state

    // collect all alive cells in the final world
    let mut alive_cells: Vec<CellCoord<usize>> = Vec::new();
    for y in 0..height {
        for x in 0..width {
            let idx = y * width + x;
            if current[idx] == CellValue::Alive {
                alive_cells.push(CellCoord { x, y });
            }
        }
    }

    // Tell SDL & tests that this is the final turn and final world
    events.send(Event::FinalTurnComplete {
        completed_turns,
        alive: alive_cells,
    })?;

    // output final snapshot for last completed_turns
    let output_name = format!("{}x{}x{}", width, height, completed_turns);

    // tell IO we want to output an image
    io_command.send(IoCommand::IoOutput)?;
    // send the filename to IO
    io_filename.send(output_name.clone())?;

    // send each cell to IO
    for cell in current.iter() {
        io_output.send(*cell)?;
    }

    // notify that image output has completed
    events.send(Event::ImageOutputComplete {
        completed_turns,
        filename: output_name,
    })?;

    // verify IO has finished any output before exiting
    io_command.send(IoCommand::IoCheckIdle)?;
    io_idle.recv()?;

    // finally, tell SDL/tests that we’re quitting
    events.send(Event::StateChange {
        completed_turns,
        new_state: State::Quitting,
    })?;

    Ok(())
}


// does one turn
fn do_one_turn(current: &[CellValue],next: &mut [CellValue],width: usize,height: usize,events: &Sender<Event>,stats: &Arc<Mutex<AliveStats>>,completed_turns: &mut u32,) 
-> Result<()> {
    // compute the next gen into next
    update_world_parallel_rayon(current, next, width, height);

    // find which cells flipped between current and next.
    let mut flipped_cells: Vec<CellCoord<usize>> = Vec::new();
    for y in 0..height {
        for x in 0..width {
            let idx = y * width + x;
            if current[idx] != next[idx] {
                flipped_cells.push(CellCoord { x, y }); //send Cellsflipped 
            }
        }
    }

    // turn done, increment completed_turns
    *completed_turns += 1;

    // count alive cells in the new world
    let mut alive_count = 0;
    for cell in next {
        if *cell == CellValue::Alive {
            alive_count += 1;
        }
    }
    //update stats for ticker
    {
    let mut s = stats.lock().unwrap();
    s.cells = alive_count;
    s.turns = *completed_turns;
    }


    // inform SDL/tests of flipped cells in this turn
    if !flipped_cells.is_empty() {
        events.send(Event::CellsFlipped {
            completed_turns: *completed_turns,
            cells: flipped_cells,
        })?;
    }

    // inform SDL/tests that this turn is done
    //  SDL will render a frame now.
    events.send(Event::TurnComplete {
        completed_turns: *completed_turns,
    })?;

    Ok(())
}

// helper func that counts the number of alive neighbours around the cell at (x, y).
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
            let idx = ny * width + nx;

            // If this neighbour cell is alive, increment the count.
            if world[idx] == CellValue::Alive {
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
// long story short this is the Parallel update: next <- f(current).
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
            for x in 0..width {
                let idx = y * width + x;    // global index into the flat current buffer.

                // count neighbours in the full current world.
                let alive_neighbours = count_neighbours(current, x, y, width, height);
                let cell = current[idx];

                row[x] = match (cell, alive_neighbours) {
                    (CellValue::Alive, 2) | (_, 3) => CellValue::Alive,
                    _ => CellValue::Dead,
                };
            }
        });
}



// original bench didnt work so i implemented this function
// used only for benchmarks: no IO thread, no SDL, no key presses
// it just runs the parallel world update for turns iterations on a width x height grid.
pub fn run_benchmark_world(turns: u32, width: usize, height: usize) {
    use crate::util::cell::CellValue;
    use flume;

    // allocate two worlds
    let mut current = vec![CellValue::Dead; width * height];
    let mut next = vec![CellValue::Dead; width * height];

    if width >= 3 && height >= 3 {
        let x = width / 2;
        let y = height / 2;
        current[y * width + x] = CellValue::Alive;
    }

    // we dont care about events in the benchmark.
    let (events_tx, events_rx) = flume::unbounded::<Event>();

    // shared stats for AliveCellsCount (ticker thread is NOT used here)
    let stats = Arc::new(std::sync::Mutex::new(AliveStats {
        turns: 0,
        cells: 0,
    }));

    let mut completed_turns: u32 = 0;

    for _ in 0..turns {
        // run one turn of the simulation.
        let _ = do_one_turn(
            &current,
            &mut next,
            width,
            height,
            &events_tx,
            &stats,
            &mut completed_turns,
        );

        // swap buffers so current is always the latest world.
        std::mem::swap(&mut current, &mut next);

        // drain any events so the channel doesn't grow without bound.
        while events_rx.try_recv().is_ok() {}
    }
}
