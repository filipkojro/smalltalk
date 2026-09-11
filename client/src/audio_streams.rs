use cpal::{
    Device, StreamConfig,
    traits::{DeviceTrait, StreamTrait},
};
use ringbuf::{
    HeapRb,
    traits::{Consumer, Observer, Producer, Split},
};
use ropus::Encoder;
use shared::AudioStreamInfo;
use std::sync::{Arc, RwLock};
use tokio::{
    net::UdpSocket,
    sync::broadcast::{Receiver, Sender},
};

use crate::models::{
    ClientEvent, ClientInfo, InputAudioStreamsConsumers, InputAudioStreamsEncoders,
    InputAudioStreamsInfo, InputAudioStreamsProducers,
};

pub async fn audio_stream_receiver(
    output_device: Device,
    output_device_config: StreamConfig,
    active_input_audio_streams_producers: InputAudioStreamsProducers,
    active_input_audio_streams_consumers: InputAudioStreamsConsumers,
    active_input_audio_streams_encoders: InputAudioStreamsEncoders,
    active_input_audio_streams_info: InputAudioStreamsInfo,
    udp_socket: Arc<UdpSocket>,
) {
    let output_data_fn = move |data: &mut [f32], _: &cpal::OutputCallbackInfo| {
        let mut streams = active_input_audio_streams_consumers.lock().unwrap();
        for sample in data {
            let mut mixed_value: f32 = 0.0;

            for (_key, consumer) in streams.iter_mut() {
                mixed_value += match consumer.try_pop() {
                    Some(s) => s,
                    None => 0.0,
                }
            }

            *sample = mixed_value;
        }
    };

    let err_fn = |err| eprintln!("stream error: {}", err);
    let output_stream = output_device
        .build_output_stream(output_device_config, output_data_fn, err_fn, None)
        .expect("building output stream failed");

    output_stream.play().expect("failed to play output stream");

    let mut receive_buffer = vec![0u8; 4096];

    let mut decoded_samples = [0.0f32; 1024];
    let mut decoded_samples_converted = [0.0f32; 2048];

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

                let decoders_map = active_input_audio_streams_encoders.read().unwrap();

                let decoder_mutex = decoders_map
                    .get(&(client_id, stream_id))
                    .expect("failed to get encoder mutex");

                let mut decoder = decoder_mutex.lock().expect("failed to get encoder");

                let decoded_len = decoder
                    .decode_float(
                        audio_packet,
                        &mut decoded_samples,
                        ropus::DecodeMode::Normal,
                    )
                    .expect("failed to decode opus");

                let mut active_streams_map = active_input_audio_streams_producers.lock().unwrap();

                if let Some(producer) = active_streams_map.get_mut(&(client_id, stream_id)) {
                    if stream_info.channels == output_device_config.channels {
                        producer.push_slice(
                            &decoded_samples[..decoded_len * stream_info.channels as usize],
                        );
                    } else if stream_info.channels > output_device_config.channels {
                        for (i, frame) in decoded_samples
                            [..decoded_len * stream_info.channels as usize]
                            .chunks_exact(2)
                            .enumerate()
                        {
                            decoded_samples_converted[i] = (frame[0] + frame[1]) / 2.0;
                        }

                        producer.push_slice(
                            &decoded_samples_converted
                                [..decoded_len * stream_info.channels as usize],
                        );
                    } else {
                        for (i, frame) in decoded_samples_converted
                            [..decoded_len * output_device_config.channels as usize]
                            .chunks_exact_mut(2)
                            .enumerate()
                        {
                            frame[0] = decoded_samples[i];
                            frame[1] = decoded_samples[i];
                        }

                        producer.push_slice(
                            &decoded_samples_converted
                                [..decoded_len * output_device_config.channels as usize],
                        );
                    }
                }
            }
            Err(e) => eprintln!("failed to receive udp packet: {}", e),
        }
    }
}

pub async fn audio_stream_spawner(
    input_device: Device,
    input_device_config: StreamConfig,
    global_event_tx: Sender<ClientEvent>,
    mut global_event_rx: Receiver<ClientEvent>,
    udp_socket: Arc<UdpSocket>,
    clinet_info: Arc<RwLock<ClientInfo>>,
) {
    let current_stream_id: u8 = rand::random();

    loop {
        let (connected, client_id) = {
            let info = clinet_info.read().unwrap();
            (info.connected, info.client_id)
        };
        if connected {
            let _ = global_event_tx.send(ClientEvent::NewAudioStreamRequest(AudioStreamInfo {
                client_id: client_id,
                stream_id: current_stream_id,
                channels: input_device_config.channels,
                sample_rate: input_device_config.sample_rate,
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

    let ring_buffer_ms = 150.0;
    let ring_buffer_frames = (ring_buffer_ms / 1000.0) * input_device_config.sample_rate as f32;
    let ring_buffer_samples = ring_buffer_frames as usize * input_device_config.channels as usize;
    let ring_buffer_size = ring_buffer_samples;
    let ring_buffer = HeapRb::<f32>::new(ring_buffer_size);
    let (mut producer, mut consumer) = ring_buffer.split();

    let input_data_fn = move |data: &[f32], _: &cpal::InputCallbackInfo| {
        for &sample in data {
            if producer.try_push(sample).is_err() {}
        }
    };
    let err_fn = |err| eprintln!("stream error: {}", err);

    let input_stream = input_device
        .build_input_stream(input_device_config, input_data_fn, err_fn, None)
        .expect("building input stream failed");

    input_stream.play().expect("failed to play input stream");

    let frames_needed = ((20.0 / 1000.0) * input_device_config.sample_rate as f32) as usize;
    let mut frame_buffer = vec![0.0; frames_needed];

    let channels: ropus::Channels = {
        if input_device_config.channels == 1 {
            ropus::Channels::Mono
        } else {
            ropus::Channels::Stereo
        }
    };

    let mut opus_encoder = Encoder::builder(
        input_device_config.sample_rate,
        channels,
        ropus::Application::Voip,
    )
    .build()
    .unwrap();

    let mut encoded_frame_buffer = [0u8; 1024];

    loop {
        if consumer.occupied_len() >= frames_needed {
            consumer.pop_slice(&mut frame_buffer);

            let mut payload = Vec::with_capacity(4 + 1 + frames_needed * 4);

            let (server_address, client_id) = {
                let info = clinet_info.read().unwrap();
                (info.server_address.clone(), info.client_id)
            };

            payload.extend(client_id.to_be_bytes());
            payload.extend(current_stream_id.to_be_bytes());

            let encoded_len = opus_encoder
                .encode_float(&frame_buffer.as_slice(), &mut encoded_frame_buffer)
                .expect("failed to encode frame");

            payload.extend_from_slice(&encoded_frame_buffer[..encoded_len]);

            let _ = udp_socket.send_to(&payload, server_address).await;
        } else {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    }
}
