use std::sync::{Arc, Mutex};

use dashmap::DashMap;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use rayon::{ThreadPool, ThreadPoolBuilder};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ChunkCoord {
    x: i32,
    z: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum ChunkStage {
    Empty,
    Stage1,
    Stage2,
    Stage3,
    Full,
}

#[derive(Debug, Clone)]
struct ChunkData {
    data: Vec<u8>,
}

#[derive(Debug)]
struct ChunkState {
    stage: ChunkStage,
    data: ChunkData,
}

#[derive(Debug)]
struct Chunk {
    coord: ChunkCoord,
    state: Mutex<ChunkState>,
    notify_full: Notify,
}

const fn dependency_radius(chunk_stage: ChunkStage) -> i32 {
    match chunk_stage {
        ChunkStage::Empty => 0,
        ChunkStage::Stage1 => 1,
        ChunkStage::Stage2 => 2,
        ChunkStage::Stage3 => 1,
        ChunkStage::Full => 0,
    }
}

impl Chunk {
    fn new(coord: ChunkCoord) -> Self {
        Self {
            coord,
            state: Mutex::new(ChunkState {
                stage: ChunkStage::Empty,
                data: ChunkData {
                    data: vec![0; 1024],
                },
            }),
            notify_full: Notify::new(),
        }
    }

    fn advance_to_stage(
        &self,
        target_stage: ChunkStage,
        chunks: &Arc<DashMap<ChunkCoord, Arc<Chunk>>>,
    ) {
        loop {
            let current_stage = self.state.lock().unwrap().stage;

            if current_stage >= target_stage {
                return;
            }

            let next_stage_deps = self.get_dependants(current_stage);

            next_stage_deps
                .par_iter()
                .for_each(|(coord, required_stage)| {
                    let dependency_chunk = chunks
                        .entry(*coord)
                        .or_insert_with(|| Arc::new(Chunk::new(*coord)))
                        .clone();

                    dependency_chunk.advance_to_stage(*required_stage, chunks);
                });

            let mut state = self.state.lock().unwrap();

            // Check for race conditions.
            if state.stage != current_stage {
                continue;
            }

            match state.stage {
                ChunkStage::Empty => {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    state.stage = ChunkStage::Stage1;
                }
                ChunkStage::Stage1 => {
                    // Example of using a dependency's data
                    if let Some(neighbor) = chunks.get(&ChunkCoord {
                        x: self.coord.x + 1,
                        z: self.coord.z + 1,
                    }) {
                        neighbor.state.lock().unwrap().data.data = vec![1; 1024];
                    }
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    state.stage = ChunkStage::Stage2;
                    println!("Stage 1 -> 2 advanced {:?}", self.coord);
                }
                ChunkStage::Stage2 => {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    state.stage = ChunkStage::Stage3;
                    println!("Stage 2 -> 3 advanced {:?}", self.coord);
                }
                ChunkStage::Stage3 => {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    state.stage = ChunkStage::Full;
                    println!("Stage 3 -> Full advanced {:?}", self.coord);
                    self.notify_full.notify_waiters();
                }
                ChunkStage::Full => {
                    // Should not happen due to the check at the top, but is safe.
                }
            }
        }
    }

    // Helper to get dependencies for the *next* stage transition.
    fn get_dependants(&self, current_stage: ChunkStage) -> Vec<(ChunkCoord, ChunkStage)> {
        let mut deps = Vec::new();
        let dep_radius = dependency_radius(current_stage);

        if dep_radius == 0 {
            return deps;
        }

        for dx in -dep_radius..=dep_radius {
            for dz in -dep_radius..=dep_radius {
                if dx == 0 && dz == 0 {
                    continue;
                }
                deps.push((
                    ChunkCoord {
                        x: self.coord.x + dx,
                        z: self.coord.z + dz,
                    },
                    current_stage, // The dependency must be at least at our current stage
                ));
            }
        }
        deps
    }
}

struct ChunkGenerator {
    chunks: Arc<DashMap<ChunkCoord, Arc<Chunk>>>,
    chunk_tickets: Arc<tokio::sync::Mutex<Vec<ChunkCoord>>>,
    ticket_notify: Arc<Notify>,
    thread_pool: Arc<ThreadPool>,
    handles: Arc<tokio::sync::Mutex<Vec<JoinHandle<()>>>>,
}

fn rayon_chunk_generator(chunk_coord: ChunkCoord, generator: Arc<ChunkGenerator>) {
    let chunk = generator
        .chunks
        .entry(chunk_coord)
        .or_insert_with(|| Arc::new(Chunk::new(chunk_coord)))
        .clone();

    chunk.advance_to_stage(ChunkStage::Full, &generator.chunks);

    println!("Chunk {:?} generated", chunk_coord);
}

fn into_key(stage: ChunkStage) -> &'static str {
    match stage {
        ChunkStage::Empty => "🟫",
        ChunkStage::Stage1 => "🟦",
        ChunkStage::Stage2 => "🟧",
        ChunkStage::Stage3 => "🟨",
        ChunkStage::Full => "🟩",
    }
}

fn pretty_print_chunks_around(center: ChunkCoord, radius: i32, generator: &Arc<ChunkGenerator>) {
    for z in center.z - radius..=center.z + radius {
        for x in center.x - radius..=center.x + radius {
            let coord = ChunkCoord { x, z };
            if let Some(chunk) = generator.chunks.get(&coord) {
                let stage = chunk.state.lock().unwrap().stage;
                print!("{}", into_key(stage));
            } else {
                print!("🟫");
            }
        }
        println!();
    }
}

impl ChunkGenerator {
    fn new() -> Arc<Self> {
        let generator = Arc::new(Self {
            chunks: Arc::new(DashMap::new()),
            chunk_tickets: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            ticket_notify: Arc::new(Notify::new()),
            thread_pool: Arc::new(
                ThreadPoolBuilder::new()
                    .num_threads(num_cpus::get())
                    .build()
                    .unwrap(),
            ),
            handles: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        });

        let generator_clone = generator.clone();
        tokio::spawn(async move {
            loop {
                let maybe_coord = generator.chunk_tickets.lock().await.pop();

                if let Some(coord) = maybe_coord {
                    let generator_clone = generator.clone();
                    generator.thread_pool.spawn(move || {
                        rayon_chunk_generator(coord, generator_clone);
                    });
                } else {
                    generator.ticket_notify.notified().await;
                }
            }
        });

        generator_clone
    }

    async fn request_chunk(&self, coord: ChunkCoord) {
        self.chunk_tickets.lock().await.push(coord);
        self.ticket_notify.notify_one();
    }

    pub async fn wait_for_chunk(&self, coord: ChunkCoord) -> Arc<Chunk> {
        let chunk = self
            .chunks
            .entry(coord)
            .or_insert_with(|| Arc::new(Chunk::new(coord)))
            .clone();

        loop {
            if chunk.state.lock().unwrap().stage == ChunkStage::Full {
                return chunk;
            }
            let notified = chunk.notify_full.notified();

            notified.await;
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let generator = ChunkGenerator::new();

    for x in -4..=4 {
        for z in -4..=4 {
            generator.request_chunk(ChunkCoord { x, z }).await;
        }
    }

    for x in -4..=4 {
        for z in -4..=4 {
            let _chunk = generator.wait_for_chunk(ChunkCoord { x, z }).await;
        }
    }

    println!("Final chunk states:");
    pretty_print_chunks_around(ChunkCoord { x: 0, z: 0 }, 10, &generator);

    println!("All Chunks Generated");

    Ok(())
}
