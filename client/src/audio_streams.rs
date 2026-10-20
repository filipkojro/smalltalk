use audioadapter_buffers::direct::InterleavedSlice;
use cpal::{
    Device, FromSample, SAMPLE_RATE_48K, Sample, SampleFormat, SizedSample, StreamConfig,
    SupportedStreamConfig,
    traits::{DeviceTrait, StreamTrait},
};
use ringbuf::{
    HeapRb, SharedRb,
    storage::Heap,
    traits::{Consumer, Observer, Producer, Split},
    wrap::caching::Caching,
};
use ropus::{Decoder, Encoder};
use rubato::{Fft, FixedSync, Resampler, audioadapter::Adapter};
use shared::AudioStreamInfo;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};
use tokio::{
    net::UdpSocket,
    sync::broadcast::{Receiver, Sender},
};

use crate::models::{
    ClientEvent, ClientInfo, InputAudioStreamsConsumers, InputAudioStreamsDecoders,
    InputAudioStreamsInfo, InputAudioStreamsProducers, InputAudioStreamsResamplers,
};

fn cpal_build_output_stream<T>(
    output_device: &Device,
    output_device_config: StreamConfig,
    active_input_audio_streams_consumers: InputAudioStreamsConsumers,
) -> cpal::Stream
where
    T: SizedSample,
    T: FromSample<f32>,
{
    let output_data_fn = move |data: &mut [T], _: &cpal::OutputCallbackInfo| {
        let mut streams = active_input_audio_streams_consumers.lock().unwrap();
        for sample in data {
            let mut mixed_value: f32 = 0.0;

            for (_key, consumer) in streams.iter_mut() {
                mixed_value += match consumer.try_pop() {
                    Some(s) => s,
                    None => 0.0,
                }
            }

            *sample = mixed_value.to_sample::<T>();
        }
    };
    let err_fn = |err| eprintln!("stream error: {}", err);

    return output_device
        .build_output_stream(output_device_config, output_data_fn, err_fn, None)
        .expect("building output stream failed");
}

pub async fn audio_stream_receiver(
    output_device: Device,
    output_device_config: SupportedStreamConfig,
    active_input_audio_streams_info: InputAudioStreamsInfo,
    udp_socket: Arc<UdpSocket>,
) {
    let mut active_input_audio_streams_producers: InputAudioStreamsProducers = HashMap::new();
    let active_input_audio_streams_consumers: InputAudioStreamsConsumers =
        Arc::new(Mutex::new(HashMap::new()));
    let mut active_input_audio_streams_decoders: InputAudioStreamsDecoders = HashMap::new();
    let mut active_input_audio_streams_resamplers: InputAudioStreamsResamplers = HashMap::new();

    let output_stream = match output_device_config.sample_format() {
        SampleFormat::F64 => cpal_build_output_stream::<f64>(
            &output_device,
            output_device_config.into(),
            active_input_audio_streams_consumers.clone(),
        ),
        SampleFormat::I64 => cpal_build_output_stream::<i64>(
            &output_device,
            output_device_config.into(),
            active_input_audio_streams_consumers.clone(),
        ),
        SampleFormat::U64 => cpal_build_output_stream::<u64>(
            &output_device,
            output_device_config.into(),
            active_input_audio_streams_consumers.clone(),
        ),
        SampleFormat::F32 => cpal_build_output_stream::<f32>(
            &output_device,
            output_device_config.into(),
            active_input_audio_streams_consumers.clone(),
        ),
        SampleFormat::I32 => cpal_build_output_stream::<i32>(
            &output_device,
            output_device_config.into(),
            active_input_audio_streams_consumers.clone(),
        ),
        SampleFormat::U32 => cpal_build_output_stream::<u32>(
            &output_device,
            output_device_config.into(),
            active_input_audio_streams_consumers.clone(),
        ),
        SampleFormat::I16 => cpal_build_output_stream::<i16>(
            &output_device,
            output_device_config.into(),
            active_input_audio_streams_consumers.clone(),
        ),
        SampleFormat::U16 => cpal_build_output_stream::<u16>(
            &output_device,
            output_device_config.into(),
            active_input_audio_streams_consumers.clone(),
        ),
        SampleFormat::I8 => cpal_build_output_stream::<i8>(
            &output_device,
            output_device_config.into(),
            active_input_audio_streams_consumers.clone(),
        ),
        SampleFormat::U8 => cpal_build_output_stream::<u8>(
            &output_device,
            output_device_config.into(),
            active_input_audio_streams_consumers.clone(),
        ),
        _ => {
            eprintln!("unsupported output stream format :(");
            return;
        }
    };

    output_stream.play().expect("failed to play output stream");

    let mut receive_buffer = vec![0u8; 4096];

    let max_input_raw_samples = ((20.0 / 1000.0) * 2.0 * SAMPLE_RATE_48K as f32) as usize; // 20ms, 2channels, 48khz
    let mut decoded_samples = vec![0.0f32; max_input_raw_samples];

    let mut resampled_samples = vec![0.0f32; max_input_raw_samples];

    let output_raw_samples = ((20.0 / 1000.0)
        * output_device_config.channels() as f32
        * output_device_config.sample_rate() as f32) as usize;
    let mut decoded_samples_converted = vec![0.0f32; output_raw_samples];

    loop {
        match udp_socket.recv_from(&mut receive_buffer).await {
            Ok((len, _)) => {
                if len < 4 {
                    continue;
                }

                let client_id_bytes: [u8; 4] = receive_buffer[0..4].try_into().unwrap();
                let client_id = u32::from_be_bytes(client_id_bytes);

                let stream_id_bytes = receive_buffer[4];
                let stream_id = u8::from_be(stream_id_bytes);

                let audio_packet = &receive_buffer[5..len];

                let stream_info = {
                    if let Some(&info) = active_input_audio_streams_info
                        .read()
                        .unwrap()
                        .get(&(client_id, stream_id))
                    {
                        info
                    } else {
                        continue;
                    }
                };

                let input_frames = ((20.0 / 1000.0) * stream_info.sample_rate as f32) as usize;
                let resampler_output_frames =
                    ((20.0 / 1000.0) * output_device_config.sample_rate() as f32) as usize;

                if !active_input_audio_streams_decoders.contains_key(&(client_id, stream_id)) {
                    let channels: ropus::Channels = {
                        if stream_info.channels == 2 {
                            ropus::Channels::Stereo
                        } else {
                            ropus::Channels::Mono
                        }
                    };

                    let decoder = Decoder::new(stream_info.sample_rate, channels)
                        .expect("failed to create decoder");
                    active_input_audio_streams_decoders
                        .insert((stream_info.client_id, stream_info.stream_id), decoder);
                }

                if !active_input_audio_streams_resamplers.contains_key(&(client_id, stream_id)) {
                    let resampler = Fft::<f32>::new(
                        stream_info.sample_rate as usize,
                        output_device_config.sample_rate() as usize,
                        input_frames,
                        stream_info.channels as usize,
                        FixedSync::Both,
                    )
                    .unwrap();
                    active_input_audio_streams_resamplers.insert((client_id, stream_id), resampler);
                }

                if !active_input_audio_streams_producers.contains_key(&(client_id, stream_id)) {
                    let ring_buffer_frames = (100.0 / 1000.0) * stream_info.sample_rate as f32;
                    let ring_buffer_samples =
                        ring_buffer_frames as usize * stream_info.channels as usize;
                    let ring_buffer = HeapRb::<f32>::new(ring_buffer_samples);
                    let (producer, consumer) = ring_buffer.split();

                    active_input_audio_streams_producers.insert((client_id, stream_id), producer);
                    active_input_audio_streams_consumers
                        .lock()
                        .unwrap()
                        .insert((client_id, stream_id), consumer);
                }

                let decoder = active_input_audio_streams_decoders
                    .get_mut(&(client_id, stream_id))
                    .expect("failed to get decoder");

                let _ = decoder
                    .decode_float(
                        audio_packet,
                        &mut decoded_samples,
                        ropus::DecodeMode::Normal,
                    )
                    .expect("failed to decode opus");

                let decoded_samples_adapter = InterleavedSlice::new(
                    &decoded_samples,
                    stream_info.channels as usize,
                    input_frames,
                )
                .unwrap();

                let mut resampled_samples_adapter = InterleavedSlice::new_mut(
                    &mut resampled_samples,
                    stream_info.channels as usize,
                    resampler_output_frames,
                )
                .unwrap();

                let (_, frames_out) = active_input_audio_streams_resamplers
                    .get_mut(&(client_id, stream_id))
                    .expect("failed to get resampler")
                    .process_into_buffer(
                        &decoded_samples_adapter,
                        &mut resampled_samples_adapter,
                        None,
                    )
                    .expect("error during resampling");

                if let Some(producer) =
                    active_input_audio_streams_producers.get_mut(&(client_id, stream_id))
                {
                    for (&resampled_sample, output_frame) in resampled_samples
                        .iter()
                        .step_by(stream_info.channels as usize)
                        .zip(
                            decoded_samples_converted
                                .chunks_exact_mut(output_device_config.channels() as usize),
                        )
                    {
                        for output_sample in output_frame.iter_mut() {
                            *output_sample = resampled_sample;
                        }
                    }

                    producer.push_slice(
                        &decoded_samples_converted
                            [..frames_out * output_device_config.channels() as usize],
                    );
                }
            }
            Err(e) => eprintln!("failed to receive udp packet: {}", e),
        }
    }
}

fn cpal_build_input_stream<T>(
    input_device: &Device,
    input_device_config: StreamConfig,
    mut producer: Caching<Arc<SharedRb<Heap<f32>>>, true, false>,
) -> cpal::Stream
where
    T: SizedSample,
    f32: FromSample<T>,
{
    let input_data_fn = move |data: &[T], _: &cpal::InputCallbackInfo| {
        for &sample in data {
            let f32_sample: f32 = sample.to_sample::<f32>();

            if producer.try_push(f32_sample).is_err() {}
        }
    };
    let err_fn = |err| eprintln!("stream error: {}", err);

    return input_device
        .build_input_stream(input_device_config, input_data_fn, err_fn, None)
        .expect("building input stream failed");
}

pub async fn audio_stream_spawner(
    input_device: Device,
    input_device_config: SupportedStreamConfig,
    global_event_tx: Sender<ClientEvent>,
    mut global_event_rx: Receiver<ClientEvent>,
    udp_socket: Arc<UdpSocket>,
    client_info: Arc<RwLock<ClientInfo>>,
    force_channels: u16,
) {
    let current_stream_id: u8 = rand::random();

    loop {
        let (connected, client_id) = {
            let info = client_info.read().unwrap();
            (info.connected, info.client_id)
        };
        if connected {
            let _ = global_event_tx.send(ClientEvent::NewAudioStreamRequest(AudioStreamInfo {
                client_id: client_id,
                stream_id: current_stream_id,
                channels: force_channels,
                sample_rate: SAMPLE_RATE_48K,
            }));
            break;
        }
    }

    loop {
        match global_event_rx.recv().await {
            Ok(ClientEvent::NewAudioStreamAccepted(stream_id))
                if stream_id == current_stream_id =>
            {
                println!("new stream accepted: {}", stream_id);
                break;
            }
            Ok(_) => {}
            Err(e) => eprintln!("error: {}", e),
        }
    }

    let ring_buffer_ms = 80.0;
    let ring_buffer_frames = (ring_buffer_ms / 1000.0) * input_device_config.sample_rate() as f32;
    let ring_buffer_size = ring_buffer_frames as usize * input_device_config.channels() as usize;
    let ring_buffer = HeapRb::<f32>::new(ring_buffer_size);
    let (producer, mut consumer) = ring_buffer.split();

    let input_stream = match input_device_config.sample_format() {
        SampleFormat::F64 => {
            cpal_build_input_stream::<f64>(&input_device, input_device_config.into(), producer)
        }
        SampleFormat::I64 => {
            cpal_build_input_stream::<i64>(&input_device, input_device_config.into(), producer)
        }
        SampleFormat::U64 => {
            cpal_build_input_stream::<u64>(&input_device, input_device_config.into(), producer)
        }
        SampleFormat::F32 => {
            cpal_build_input_stream::<f32>(&input_device, input_device_config.into(), producer)
        }
        SampleFormat::I32 => {
            cpal_build_input_stream::<i32>(&input_device, input_device_config.into(), producer)
        }
        SampleFormat::U32 => {
            cpal_build_input_stream::<u32>(&input_device, input_device_config.into(), producer)
        }
        SampleFormat::I16 => {
            cpal_build_input_stream::<i16>(&input_device, input_device_config.into(), producer)
        }
        SampleFormat::U16 => {
            cpal_build_input_stream::<u16>(&input_device, input_device_config.into(), producer)
        }
        SampleFormat::I8 => {
            cpal_build_input_stream::<i8>(&input_device, input_device_config.into(), producer)
        }
        SampleFormat::U8 => {
            cpal_build_input_stream::<u8>(&input_device, input_device_config.into(), producer)
        }
        _ => {
            eprintln!("unsupported input stream format :(");
            return;
        }
    };

    input_stream.play().expect("failed to play input stream");

    let frames_needed = ((20.0 / 1000.0) * input_device_config.sample_rate() as f32) as usize;

    let samples_needed = frames_needed * input_device_config.channels() as usize;

    let mut frame_buffer = vec![0f32; samples_needed];

    let mut resampler_input_buffer = vec![0.0f32; frames_needed * force_channels as usize];

    let mut resampler = Fft::<f32>::new(
        input_device_config.sample_rate() as usize,
        SAMPLE_RATE_48K as usize,
        frames_needed,
        force_channels as usize,
        FixedSync::Both,
    )
    .unwrap();

    let mut resampler_output_buffer =
        vec![0.0f32; resampler.output_frames_max() * force_channels as usize];

    let encoder_channels: ropus::Channels = match force_channels {
        1 => ropus::Channels::Mono,
        2 => ropus::Channels::Stereo,
        _ => {
            eprintln!("unsupported by encoder channel count");
            return;
        }
    };

    let mut opus_encoder =
        Encoder::builder(SAMPLE_RATE_48K, encoder_channels, ropus::Application::Voip)
            .build()
            .unwrap();

    let mut encoded_frame_buffer = [0u8; 2048];

    loop {
        if consumer.occupied_len() >= samples_needed {
            consumer.pop_slice(&mut frame_buffer);

            let frame_buffer_adapter = InterleavedSlice::new(
                &frame_buffer,
                input_device_config.channels() as usize,
                frames_needed,
            )
            .unwrap();

            frame_buffer_adapter.copy_from_channel_to_slice(0, 0, &mut resampler_input_buffer); // im only using first channel for now

            let resampler_input = InterleavedSlice::new(
                &resampler_input_buffer,
                force_channels as usize,
                frames_needed,
            )
            .unwrap();

            let mut resampler_output = InterleavedSlice::new_mut(
                &mut resampler_output_buffer,
                force_channels as usize,
                resampler.output_frames_max(),
            )
            .unwrap();

            let (_frames_read, frames_written) = resampler
                .process_into_buffer(&resampler_input, &mut resampler_output, None)
                .unwrap();

            let valid_samples = frames_written * force_channels as usize;

            let encoded_len = opus_encoder
                .encode_float(
                    &resampler_output_buffer[..valid_samples],
                    &mut encoded_frame_buffer,
                )
                .expect("failed to encode frame");

            let mut payload = Vec::<u8>::with_capacity(4 + 1 + encoded_len * 4);

            let (server_address, client_id) = {
                let info = client_info.read().unwrap();
                (info.server_address.clone(), info.client_id)
            };

            payload.extend(client_id.to_be_bytes());
            payload.extend(current_stream_id.to_be_bytes());

            payload.extend_from_slice(&encoded_frame_buffer[..encoded_len]);

            let _ = udp_socket.send_to(&payload, server_address).await;
        } else {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    }
}
