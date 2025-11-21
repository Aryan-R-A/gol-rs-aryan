use clap::Parser;
use gol_rs::args::Args;
use gol_rs::gol;
use gol_rs::gol::event::Event;
use gol_rs::sdl;
use gol_rs::util::logger;
use log::Level;
use sdl2::keyboard::Keycode;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::try_join;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args = Args::parse();
    logger::init(Level::Info, false);

    log::info!(target: "Main", "{:<10} {}", "Threads", args.threads);
    log::info!(target: "Main", "{:<10} {}", "Width", args.image_width);
    log::info!(target: "Main", "{:<10} {}", "Height", args.image_height);
    log::info!(target: "Main", "{:<10} {}", "Turns", args.turns);

    let (key_presses_tx, key_presses_rx) = flume::bounded::<Keycode>(10);
    let (events_tx, events_rx) = flume::bounded::<Event>(1000);

    tokio::spawn(sigint());

    if !args.headless {
        try_join!(
            gol::run(args.clone(), events_tx, key_presses_rx),
            sdl::r#loop::run(args, events_rx, key_presses_tx)
        ).unwrap();
    } else {
        try_join!(
            gol::run(args, events_tx, key_presses_rx),
            sdl::r#loop::run_headless(events_rx)
        ).unwrap();
    }
}

async fn sigint() {
    let exit = Arc::new(AtomicBool::new(false));
    loop {
        tokio::signal::ctrl_c().await.unwrap();
        if exit.load(Ordering::SeqCst) {
            log::warn!(target: "Main", "Force quit by the user");
            std::process::exit(0);
        } else {
            log::warn!(target: "Main", "Press Ctrl+C again to force quit");
            exit.store(true, Ordering::SeqCst);
            tokio::spawn({
                let exit = Arc::clone(&exit);
                async move {
                    tokio::time::sleep(Duration::from_secs(4)).await;
                    exit.store(false, Ordering::SeqCst);
                }
            });
        }
    }
}
