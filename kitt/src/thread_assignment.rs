/// Thread assignment utilities for the KITT application
///
/// This module provides utilities for calculating thread assignments
/// based on the threads-per-partition model.

/// Calculates thread assignment details for a given thread ID and configuration
///
/// # Arguments
/// * `thread_id` - The ID of the thread (0-based)
/// * `threads_per_partition` - Number of threads assigned to each partition
///
/// # Returns
/// A ThreadAssignment struct with calculated assignment details
pub fn calculate_thread_assignment(
    thread_id: usize,
    threads_per_partition: usize,
) -> ThreadAssignment {
    ThreadAssignment {
        thread_id,
        partition_id: (thread_id / threads_per_partition) as i32,
        thread_within_partition: thread_id % threads_per_partition,
        total_threads_for_partition: threads_per_partition,
    }
}

/// Calculates the total number of threads needed for a given configuration
///
/// # Arguments
/// * `partitions` - Number of partitions
/// * `threads_per_partition` - Number of threads per partition
/// * `include_consumers` - Whether to include consumer threads in the count (doubles the result)
///
/// # Returns
/// The total number of threads required
pub fn calculate_total_threads(
    partitions: i32,
    threads_per_partition: usize,
    include_consumers: bool,
) -> usize {
    let producer_threads = (partitions as usize) * threads_per_partition;
    if include_consumers {
        producer_threads * 2
    } else {
        producer_threads
    }
}

/// Represents the assignment of a thread to a specific partition
#[derive(Debug, Clone, PartialEq)]
pub struct ThreadAssignment {
    /// The ID of the thread (0-based)
    pub thread_id: usize,
    /// The partition this thread is assigned to
    pub partition_id: i32,
    /// The thread's index within its assigned partition (0-based)
    pub thread_within_partition: usize,
    /// Total number of threads assigned to the same partition
    pub total_threads_for_partition: usize,
}
