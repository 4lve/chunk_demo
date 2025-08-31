use std::sync::Arc;

use dashmap::DashMap;
use rayon::iter::ParallelIterator;
use rayon::{ThreadPool, ThreadPoolBuilder, iter::IntoParallelRefIterator};
use tokio::sync::{Mutex, Notify};
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
    Full,
}

#[derive(Debug, Clone)]
struct ChunkData {
    data: Vec<u8>,
}

#[derive(Debug)]
struct PendingChunk {
    data: std::sync::Mutex<ChunkData>,
    stage: std::sync::Mutex<ChunkStage>,
    coord: ChunkCoord,
}

const fn dependency_radius(chunk_stage: ChunkStage) -> i32 {
    match chunk_stage {
        ChunkStage::Empty => 0,
        ChunkStage::Stage1 => 1,
        ChunkStage::Stage2 => 1,
        ChunkStage::Full => 0,
    }
}

impl PendingChunk {
    fn new(coord: ChunkCoord) -> Self {
        Self {
            data: std::sync::Mutex::new(ChunkData {
                data: vec![0; 1024],
            }),
            stage: std::sync::Mutex::new(ChunkStage::Empty),
            coord,
        }
    }

    // Simulate, empty -> stage 1 (100ms) -> full (100ms)
    fn advance_to_stage(
        &self,
        stage: ChunkStage,
        pending_chunks: &Arc<DashMap<ChunkCoord, Arc<PendingChunk>>>,
    ) {
        while *self.stage.lock().unwrap() < stage {
            self.advance(pending_chunks);
        }
    }

    fn advance(&self, pending_chunks: &Arc<DashMap<ChunkCoord, Arc<PendingChunk>>>) {
        let mut self_stage = self.stage.lock().unwrap();
        match *self_stage {
            ChunkStage::Empty => {
                std::thread::sleep(std::time::Duration::from_millis(100));
                *self_stage = ChunkStage::Stage1;
            }
            ChunkStage::Stage1 => {
                drop(self_stage);

                self.dependants().par_iter().for_each(|(coord, stage)| {
                    let pending_chunk = {
                        pending_chunks
                            .entry(*coord)
                            .or_insert(Arc::new(PendingChunk::new(*coord)))
                            .clone()
                    };
                    println!("Advancing dependant chunk {:?} to stage {:?}", coord, stage);
                    pending_chunk.advance_to_stage(*stage, pending_chunks);
                });

                std::thread::sleep(std::time::Duration::from_millis(100));
                *self.stage.lock().unwrap() = ChunkStage::Stage2;
            }
            ChunkStage::Stage2 => {
                drop(self_stage);

                self.dependants().par_iter().for_each(|(coord, stage)| {
                    let pending_chunk = pending_chunks.get(coord).unwrap().clone();
                    pending_chunk.advance_to_stage(*stage, pending_chunks);
                });

                std::thread::sleep(std::time::Duration::from_millis(100));
                *self.stage.lock().unwrap() = ChunkStage::Full;
            }
            ChunkStage::Full => {
                // No advance
            }
        }
    }

    fn dependants(&self) -> Vec<(ChunkCoord, ChunkStage)> {
        /*

        // 0 = empty
        // 1 = stage 1
        // 2 = full

        00000
        00000
        00000
        00000
        00000

        00000
        01110
        01110
        01110
        00000

        00000
        01110
        01210
        01110
        00000
         */

        let mut deps = Vec::new();

        let self_stage = self.stage.lock().unwrap().clone();
        match self_stage {
            ChunkStage::Empty => {
                // For stage 1, we need all chunks in a 3x3 grid around us to be at least Empty
                for dx in -1..=1 {
                    for dz in -1..=1 {
                        if dx == 0 && dz == 0 {
                            continue;
                        } // Skip self
                        deps.push((
                            ChunkCoord {
                                x: self.coord.x + dx,
                                z: self.coord.z + dz,
                            },
                            ChunkStage::Empty,
                        ));
                    }
                }
            }
            ChunkStage::Stage1 => {
                // For full generation, we need all chunks in a 3x3 grid to be at Stage1
                for dx in -1..=1 {
                    for dz in -1..=1 {
                        if dx == 0 && dz == 0 {
                            continue;
                        } // Skip self
                        deps.push((
                            ChunkCoord {
                                x: self.coord.x + dx,
                                z: self.coord.z + dz,
                            },
                            ChunkStage::Stage1,
                        ));
                    }
                }
            }
            ChunkStage::Stage2 => {
                // For full generation, we need all chunks in a 3x3 grid to be at Stage1
                for dx in -1..=1 {
                    for dz in -1..=1 {
                        if dx == 0 && dz == 0 {
                            continue;
                        } // Skip self
                        deps.push((
                            ChunkCoord {
                                x: self.coord.x + dx,
                                z: self.coord.z + dz,
                            },
                            ChunkStage::Stage2,
                        ));
                    }
                }
            }
            ChunkStage::Full => {
                // No dependencies when already full
            }
        }

        deps
    }
}

struct ChunkGenerator {
    loaded_chunks: DashMap<ChunkCoord, ChunkData>,
    pending_chunks: Arc<DashMap<ChunkCoord, Arc<PendingChunk>>>,
    chunk_tickets: Arc<Mutex<Vec<ChunkCoord>>>,
    ticket_notify: Arc<Notify>,
    thread_pool: Arc<ThreadPool>,
    handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

fn rayon_chunk_generator(chunk_coord: ChunkCoord, generator: Arc<ChunkGenerator>) {
    let pending_chunk = {
        generator
            .pending_chunks
            .entry(chunk_coord)
            .or_insert(Arc::new(PendingChunk::new(chunk_coord)))
            .clone()
    };

    pending_chunk.advance_to_stage(ChunkStage::Full, &generator.pending_chunks);

    // Pretty print the pending chunks in a grid format
    pretty_print_chunks_around(chunk_coord, 3, &generator);
}

fn into_key(stage: ChunkStage) -> &'static str {
    match stage {
        ChunkStage::Empty => " 0️⃣",
        ChunkStage::Stage1 => " 1️⃣",
        ChunkStage::Stage2 => " 2️⃣",
        ChunkStage::Full => " 3️⃣",
    }
}

fn pretty_print_chunks_around(center: ChunkCoord, radius: i32, generator: &Arc<ChunkGenerator>) {
    for x in center.x - radius..=center.x + radius {
        for z in center.z - radius..=center.z + radius {
            let coord = ChunkCoord { x, z };
            let chunk = generator.pending_chunks.get(&coord);
            if let Some(chunk) = chunk {
                print!("{}", into_key(*chunk.stage.lock().unwrap()));
            } else {
                print!(" #️⃣");
            }
        }
        println!();
    }
}

impl ChunkGenerator {
    fn new() -> Arc<Self> {
        let generator = Arc::new(Self {
            loaded_chunks: DashMap::new(),
            pending_chunks: Arc::new(DashMap::new()),
            chunk_tickets: Arc::new(Mutex::new(Vec::new())),
            ticket_notify: Arc::new(Notify::new()),
            thread_pool: Arc::new(
                ThreadPoolBuilder::new()
                    .num_threads(num_cpus::get())
                    .build()
                    .unwrap(),
            ),
            handles: Arc::new(Mutex::new(Vec::new())),
        });

        let generator_clone = generator.clone();
        let thread_pool_clone = generator.thread_pool.clone();
        let handle = tokio::spawn(async move {
            loop {
                if let Some(coord) = generator.chunk_tickets.lock().await.pop() {
                    let generator_clone = generator.clone();
                    thread_pool_clone.spawn(move || {
                        rayon_chunk_generator(coord, generator_clone);
                    });
                } else {
                    generator.ticket_notify.notified().await;
                }
            }
        });

        generator_clone.handles.try_lock().unwrap().push(handle);

        generator_clone
    }

    async fn request_chunk(&self, coord: ChunkCoord) -> Result<(), String> {
        let mut tickets = self.chunk_tickets.lock().await;
        tickets.push(coord);
        self.ticket_notify.notify_one();
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let generator = ChunkGenerator::new();

    let chunk = generator
        .request_chunk(ChunkCoord { x: 0, z: 0 })
        .await
        .unwrap();
    println!("Chunk: {:?}", chunk);

    for handle in generator.handles.lock().await.drain(..) {
        handle.await.unwrap();
    }

    println!("All Chunks Generated");

    Ok(())
}
