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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ChunkStage {
    Empty,
    Stage1,
    Stage2,
    Full,
}

#[derive(Debug, Clone)]
struct ChunkData {
    data: Vec<u8>,
    coord: ChunkCoord,
}

#[derive(Debug)]
struct PendingChunk {
    data: ChunkData,
    stage: ChunkStage,
}

impl PendingChunk {
    fn new(coord: ChunkCoord) -> Self {
        Self {
            data: ChunkData {
                data: vec![0; 1024],
                coord,
            },
            stage: ChunkStage::Empty,
        }
    }

    // Simulate, empty -> stage 1 (100ms) -> full (100ms)
    fn advance_to_stage(&mut self, stage: ChunkStage) {
        match stage {
            ChunkStage::Stage1 => {
                if self.stage == ChunkStage::Empty {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    self.stage = ChunkStage::Stage1;
                } else {
                    panic!("Bad planning");
                }
            }
            ChunkStage::Stage2 => {
                if self.stage == ChunkStage::Stage1 {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    self.stage = ChunkStage::Stage2;
                } else {
                    panic!("Bad planning");
                }
            }
            ChunkStage::Full => {
                if self.stage == ChunkStage::Stage2 {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    self.stage = ChunkStage::Full;
                } else {
                    panic!("Bad planning");
                }
            }
            _ => {}
        }
    }

    fn advance(&mut self) {
        match self.stage {
            ChunkStage::Empty => {
                self.advance_to_stage(ChunkStage::Stage1);
            }
            ChunkStage::Stage1 => {
                self.advance_to_stage(ChunkStage::Stage2);
            }
            ChunkStage::Stage2 => {
                self.advance_to_stage(ChunkStage::Full);
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

        match self.stage {
            ChunkStage::Empty => {
                // For stage 1, we need all chunks in a 3x3 grid around us to be at least Empty
                for dx in -1..=1 {
                    for dz in -1..=1 {
                        if dx == 0 && dz == 0 {
                            continue;
                        } // Skip self
                        deps.push((
                            ChunkCoord {
                                x: self.data.coord.x + dx,
                                z: self.data.coord.z + dz,
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
                                x: self.data.coord.x + dx,
                                z: self.data.coord.z + dz,
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
                                x: self.data.coord.x + dx,
                                z: self.data.coord.z + dz,
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
    pending_chunks: DashMap<ChunkCoord, PendingChunk>,
    chunk_tickets: Arc<Mutex<Vec<ChunkCoord>>>,
    ticket_notify: Arc<Notify>,
    thread_pool: Arc<ThreadPool>,
    handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

fn rayon_chunk_generator(chunk_coord: ChunkCoord, generator: Arc<ChunkGenerator>) {
    loop {
        let dependants = {
            generator
                .pending_chunks
                .entry(chunk_coord)
                .or_insert(PendingChunk::new(chunk_coord))
                .dependants()
        };

        dependants.par_iter().for_each(|(coord, stage)| {
            let mut pending_chunk = generator
                .pending_chunks
                .entry(*coord)
                .or_insert(PendingChunk::new(*coord));

            pending_chunk.advance_to_stage(*stage);
        });

        let mut pending_chunk = generator.pending_chunks.get_mut(&chunk_coord).unwrap();

        pending_chunk.advance();

        if pending_chunk.stage == ChunkStage::Full {
            drop(pending_chunk);

            let pending_chunk = generator.pending_chunks.remove(&chunk_coord).unwrap();
            generator
                .loaded_chunks
                .insert(chunk_coord, pending_chunk.1.data);
            break;
        }
    }

    // Pretty print the pending chunks in a grid format
    pretty_print_chunks_around(chunk_coord, 2, &generator);
}

fn pretty_print_chunks_around(center: ChunkCoord, radius: i32, generator: &Arc<ChunkGenerator>) {
    for x in center.x - radius..=center.x + radius {
        for z in center.z - radius..=center.z + radius {
            let coord = ChunkCoord { x, z };
            let chunk = generator.pending_chunks.get(&coord);
            if let Some(chunk) = chunk {
                print!("{}", chunk.stage as u8);
            } else {
                print!(".");
            }
        }
        println!();
    }
}

impl ChunkGenerator {
    fn new() -> Arc<Self> {
        let generator = Arc::new(Self {
            loaded_chunks: DashMap::new(),
            pending_chunks: DashMap::new(),
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
