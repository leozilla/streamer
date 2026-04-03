use tracing::{trace};

use crate::Port;

pub fn process_stream_data(sink: Port, tx_chan: &tokio::sync::mpsc::UnboundedSender<Vec<u8>>, buffer: [u8; 1024], n: usize) {
    let data = buffer[..n].to_vec();
    let tx_inner = tx_chan.clone();

    // HAND OFF TO RAYON (CPU bound)
    // We use a oneshot-style handoff or spawn into the global pool
    rayon::spawn(move || {
        // let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        // encoder.write_all(&data_chunk).unwrap();
        // let compressed = encoder.finish().unwrap();
    
        let _ = tx_inner.send(data);

        trace!("Data processed, sending to sink {}", sink);
    });
}