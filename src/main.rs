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
    Stage3,
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
    generation_symaphore: std::sync::Mutex<()>,
}

const fn dependency_radius(chunk_stage: ChunkStage) -> i32 {
    match chunk_stage {
        ChunkStage::Empty => 0,
        ChunkStage::Stage1 => 1,
        ChunkStage::Stage2 => 1,
        ChunkStage::Stage3 => 1,
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
            generation_symaphore: std::sync::Mutex::new(()),
        }
    }

    // Simulate, empty -> stage 1 (100ms) -> full (100ms)
    fn advance_to_stage(&self, stage: ChunkStage, chunks: &Arc<DashMap<ChunkCoord, ChunkState>>) {
        loop {
            let current_stage = {
                let stage_guard = self.stage.lock().unwrap();
                *stage_guard
            };
            if current_stage >= stage {
                break;
            }

            let advanced = self.advance(chunks, current_stage);
            if !advanced {
                let _lock = self.generation_symaphore.lock().unwrap();
                continue;
            }
        }
    }

    fn advance(
        &self,
        chunks: &Arc<DashMap<ChunkCoord, ChunkState>>,
        expected_current_stage: ChunkStage,
    ) -> bool {
        let lock = self.generation_symaphore.lock();
        let self_stage = self.stage.lock().unwrap();

        if *self_stage != expected_current_stage {
            return false; // Stage has changed since we checked
        }
        match *self_stage {
            ChunkStage::Empty => {
                drop(self_stage);
                std::thread::sleep(std::time::Duration::from_millis(100));
                *self.stage.lock().unwrap() = ChunkStage::Stage1;
                drop(lock);
                true
            }
            ChunkStage::Stage1 => {
                drop(self_stage);

                self.dependants().par_iter().for_each(|(coord, stage)| {
                    let pending_chunk = {
                        let chunk_entry = chunks
                            .entry(*coord)
                            .or_insert(ChunkState::Pending(Arc::new(PendingChunk::new(*coord))));
                        match chunk_entry.value() {
                            ChunkState::Pending(chunk) => chunk.clone(),
                            ChunkState::Loaded(_) => panic!(
                                "Chunk {:?} is already loaded (Can't be at this stage)",
                                coord
                            ),
                        }
                    };
                    pending_chunk.advance_to_stage(*stage, chunks);
                });

                match chunks
                    .get(&ChunkCoord {
                        x: self.coord.x + 1,
                        z: self.coord.z + 1,
                    })
                    .unwrap()
                    .value()
                {
                    ChunkState::Pending(chunk) => {
                        chunk.data.lock().unwrap().data = vec![1; 1024];
                    }
                    ChunkState::Loaded(_) => {
                        unreachable!();
                    }
                }

                std::thread::sleep(std::time::Duration::from_millis(100));

                *self.stage.lock().unwrap() = ChunkStage::Stage2;
                drop(lock);
                println!("Stage 1 advanced {:?}", self.coord);
                true
            }
            ChunkStage::Stage2 => {
                drop(self_stage);

                self.dependants().iter().for_each(|(coord, stage)| {
                    let pending_chunk = {
                        let chunk_entry = chunks
                            .entry(*coord)
                            .or_insert(ChunkState::Pending(Arc::new(PendingChunk::new(*coord))));
                        match chunk_entry.value() {
                            ChunkState::Pending(chunk) => chunk.clone(),
                            ChunkState::Loaded(_) => return,
                        }
                    };
                    pending_chunk.advance_to_stage(*stage, chunks);
                });

                std::thread::sleep(std::time::Duration::from_millis(100));
                *self.stage.lock().unwrap() = ChunkStage::Stage3;
                drop(lock);
                println!("Stage 2 advanced {:?}", self.coord);
                true
            }
            ChunkStage::Stage3 => {
                drop(self_stage);

                self.dependants().iter().for_each(|(coord, stage)| {
                    let pending_chunk = {
                        let chunk_entry = chunks
                            .entry(*coord)
                            .or_insert(ChunkState::Pending(Arc::new(PendingChunk::new(*coord))));
                        match chunk_entry.value() {
                            ChunkState::Pending(chunk) => chunk.clone(),
                            ChunkState::Loaded(_) => return,
                        }
                    };
                    pending_chunk.advance_to_stage(*stage, chunks);
                });

                std::thread::sleep(std::time::Duration::from_millis(100));
                *self.stage.lock().unwrap() = ChunkStage::Full;
                drop(lock);
                println!("Stage 3 advanced {:?}", self.coord);
                true
            }
            ChunkStage::Full => {
                // Already at max stage, no advancement needed
                drop(lock);
                true
            }
        }
    }

    fn dependants(&self) -> Vec<(ChunkCoord, ChunkStage)> {
        let mut deps = Vec::new();

        let self_stage = self.stage.lock().unwrap().clone();
        match self_stage {
            ChunkStage::Empty => {
                // No dependencies when empty
            }
            ChunkStage::Stage1 => {
                // For full generation, we need all chunks in a 3x3 grid to be at Stage1
                for dx in -dependency_radius(self_stage)..=dependency_radius(self_stage) {
                    for dz in -dependency_radius(self_stage)..=dependency_radius(self_stage) {
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
                for dx in -dependency_radius(self_stage)..=dependency_radius(self_stage) {
                    for dz in -dependency_radius(self_stage)..=dependency_radius(self_stage) {
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
            ChunkStage::Stage3 => {
                // For full generation, we need all chunks in a 3x3 grid to be at Stage2
                for dx in -dependency_radius(self_stage)..=dependency_radius(self_stage) {
                    for dz in -dependency_radius(self_stage)..=dependency_radius(self_stage) {
                        if dx == 0 && dz == 0 {
                            continue;
                        } // Skip self
                        deps.push((
                            ChunkCoord {
                                x: self.coord.x + dx,
                                z: self.coord.z + dz,
                            },
                            ChunkStage::Stage3,
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

#[derive(Debug)]
enum ChunkState {
    Pending(Arc<PendingChunk>),
    Loaded(ChunkData),
}

struct ChunkGenerator {
    chunks: Arc<DashMap<ChunkCoord, ChunkState>>,
    chunk_tickets: Arc<Mutex<Vec<ChunkCoord>>>,
    ticket_notify: Arc<Notify>,
    thread_pool: Arc<ThreadPool>,
    handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

fn rayon_chunk_generator(chunk_coord: ChunkCoord, generator: Arc<ChunkGenerator>) {
    let pending_chunk = {
        let chunk_entry = generator
            .chunks
            .entry(chunk_coord)
            .or_insert(ChunkState::Pending(Arc::new(PendingChunk::new(
                chunk_coord,
            ))));

        match chunk_entry.value() {
            ChunkState::Pending(chunk) => Some(chunk.clone()),
            ChunkState::Loaded(_) => None,
        }
    };

    if let Some(chunk) = pending_chunk {
        chunk.advance_to_stage(ChunkStage::Full, &generator.chunks);

        generator.chunks.insert(
            chunk_coord,
            ChunkState::Loaded(chunk.data.lock().unwrap().clone()),
        );

        println!("Chunk {:?} generated", chunk_coord);
    } else {
        println!("Chunk {:?} already loaded", chunk_coord);
    }
}

fn into_key(stage: ChunkStage) -> &'static str {
    match stage {
        ChunkStage::Empty => " 0️⃣",
        ChunkStage::Stage1 => " 1️⃣",
        ChunkStage::Stage2 => " 2️⃣",
        ChunkStage::Stage3 => " 3️⃣",
        ChunkStage::Full => " 4️⃣",
    }
}

fn pretty_print_chunks_around(center: ChunkCoord, radius: i32, generator: &Arc<ChunkGenerator>) {
    for x in center.x - radius..=center.x + radius {
        for z in center.z - radius..=center.z + radius {
            let coord = ChunkCoord { x, z };
            let chunk = generator.chunks.get(&coord);
            if let Some(chunk) = chunk {
                if let ChunkState::Pending(chunk) = chunk.value() {
                    print!("{}", into_key(*chunk.stage.lock().unwrap()));
                } else {
                    print!(" 9️⃣");
                }
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
            chunks: Arc::new(DashMap::new()),
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

    for x in -1..=1 {
        for z in -1..=1 {
            generator.request_chunk(ChunkCoord { x, z }).await.unwrap();
        }
    }

    tokio::time::sleep(std::time::Duration::from_secs(6)).await;

    pretty_print_chunks_around(ChunkCoord { x: 0, z: 0 }, 6, &generator);

    for handle in generator.handles.lock().await.drain(..) {
        handle.await.unwrap();
    }

    println!("All Chunks Generated");

    Ok(())
}
