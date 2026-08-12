use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use std::sync::mpsc;

fn main() {
    let host = cpal::default_host();

    let input_device = host
        .default_input_device()
        .expect("no input device available");
    let output_device = host
        .default_output_device()
        .expect("no output device available");

    println!("input device: {}", input_device);
    println!("output device: {}", output_device);

    let config: cpal::StreamConfig = input_device
        .default_input_config()
        .expect("stream config init failed")
        .into();

    println!("stream config: {:?}", config);

    let latency_ms = 150.0;
    let latency_frames = (latency_ms / 1000.0) * config.sample_rate as f32;
    let latency_samples = latency_frames as usize * config.channels as usize;
    let (tx, rx) = mpsc::sync_channel::<f32>(latency_samples * 2);

    for _ in 0..latency_samples {
        tx.send(0.0).unwrap();
    }

    let input_data_fn = move |data: &[f32], _: &cpal::InputCallbackInfo| {
        for &sample in data {
            if tx.try_send(sample).is_err() {}
        }
    };

    let output_data_fn = move |data: &mut [f32], _: &cpal::OutputCallbackInfo| {
        for sample in data {
            *sample = match rx.try_recv() {
                Ok(s) => s,
                Err(_) => 0.0,
            };
        }
    };

    let err_fn = |err| eprintln!("stream error: {}", err);

    let input_stream = input_device
        .build_input_stream(config, input_data_fn, err_fn, None)
        .expect("building input stream failed");
    let output_stream = output_device
        .build_output_stream(config, output_data_fn, err_fn, None)
        .expect("building output stream failed");

    input_stream.play().expect("failed to play input stream");
    output_stream.play().expect("failed to play output stream");

    std::thread::sleep(std::time::Duration::from_secs(5));
}
