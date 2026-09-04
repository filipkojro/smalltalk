use cpal::{
    Device, SAMPLE_RATE_48K, StreamConfig,
    traits::{DeviceTrait, HostTrait},
};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind};
use gag::Gag;
use ratatui::{
    DefaultTerminal, Frame,
    layout::{Constraint, Direction, Layout},
    style::Style,
    widgets::{Block, Row, Table, TableState},
};
use std::{sync::Arc, vec};
use tokio::net::UdpSocket;

use crate::models::{
    InputAudioStreamsConsumers, InputAudioStreamsEncoders, InputAudioStreamsInfo,
    InputAudioStreamsProducers, TuiScreen,
};

pub struct Tui {
    current_screen: TuiScreen,
}

impl Tui {
    pub fn new() -> Self {
        Self {
            current_screen: TuiScreen::DeviceSelect,
        }
    }

    pub fn run(
        &mut self,
        terminal: &mut DefaultTerminal,
        active_input_audio_streams_producers: InputAudioStreamsProducers,
        active_input_audio_streams_consumers: InputAudioStreamsConsumers,
        active_input_audio_streams_encoders: InputAudioStreamsEncoders,
        active_input_audio_streams_info: InputAudioStreamsInfo,
        udp_socket: Arc<UdpSocket>,
    ) {
        loop {
            match self.current_screen {
                TuiScreen::Exit => {
                    break;
                }
                TuiScreen::DeviceSelect => {
                    self.current_screen = DeviceSelectScreen::new().run(terminal);
                }
            }
        }
    }
}

#[derive(Debug)]
struct DeviceSelectScreen {
    exit: bool,
    input_device_state: TableState,
    output_device_state: TableState,
    input_devices: Vec<(Device, StreamConfig)>,
    output_devices: Vec<(Device, StreamConfig)>,
    selected_selection: u8,
}

impl DeviceSelectScreen {
    pub fn new() -> Self {
        let _print_gag = Gag::stderr().ok();

        let host = cpal::default_host();

        let mut input_devices: Vec<(Device, StreamConfig)> = Vec::new();

        if let Ok(possible_input_devices) = host.input_devices() {
            for possible_input_device in possible_input_devices {
                if let Ok(supported_configs) = possible_input_device.supported_input_configs() {
                    for supported_config in supported_configs {
                        if let Some(input_config) =
                            supported_config.try_with_sample_rate(SAMPLE_RATE_48K)
                        {
                            input_devices.push((possible_input_device, input_config.into()));
                            break;
                        }
                    }
                }
            }
        }

        let mut output_devices: Vec<(Device, StreamConfig)> = Vec::new();

        if let Ok(possible_output_devices) = host.output_devices() {
            for possible_output_device in possible_output_devices {
                if let Ok(supported_configs) = possible_output_device.supported_output_configs() {
                    for supported_config in supported_configs {
                        if let Some(output_config) =
                            supported_config.try_with_sample_rate(SAMPLE_RATE_48K)
                        {
                            output_devices.push((possible_output_device, output_config.into()));
                            break;
                        }
                    }
                }
            }
        }

        Self {
            exit: false,
            input_device_state: TableState::default(),
            output_device_state: TableState::default(),
            input_devices: input_devices,
            output_devices: output_devices,
            selected_selection: 0,
        }
    }

    pub fn run(&mut self, terminal: &mut DefaultTerminal) -> TuiScreen {
        while !self.exit {
            terminal
                .draw(|frame| self.draw(frame))
                .expect("failed to draw");
            self.handle_events();
        }

        return TuiScreen::Exit;
    }

    fn draw(&mut self, frame: &mut Frame) {
        let outer_layout = Layout::default()
            .direction(Direction::Horizontal)
            .constraints(vec![Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(frame.area());

        let block = Block::new();
        let left_block = block.inner(outer_layout[0]);
        let right_block = block.inner(outer_layout[1]);

        let widths = [
            Constraint::Percentage(100),
            Constraint::Min(8),
            Constraint::Min(11),
        ];

        let mut input_device_rows: Vec<Row> = Vec::new();
        for (device, config) in self.input_devices.iter() {
            input_device_rows.push(Row::new(vec![
                device.description().unwrap().name().to_string(),
                config.channels.to_string(),
                config.sample_rate.to_string(),
            ]));
        }
        let input_device_table = Table::new(input_device_rows, widths)
            .header(
                Row::new(vec!["device name", "channels", "sample rate"])
                    .style(Style::new().bold())
                    .bottom_margin(1),
            )
            .block(
                Block::bordered()
                    .border_style({
                        if self.selected_selection == 0 {
                            Style::new().bold()
                        } else {
                            Style::default()
                        }
                    })
                    .border_type({
                        if self.selected_selection == 0 {
                            ratatui::widgets::BorderType::Thick
                        } else {
                            ratatui::widgets::BorderType::Plain
                        }
                    })
                    .title("select input device"),
            )
            .row_highlight_style(Style::new().reversed());

        frame.render_stateful_widget(input_device_table, left_block, &mut self.input_device_state);

        let mut output_device_rows: Vec<Row> = Vec::new();
        for (device, config) in self.output_devices.iter() {
            output_device_rows.push(Row::new(vec![
                device.description().unwrap().name().to_string(),
                config.channels.to_string(),
                config.sample_rate.to_string(),
            ]));
        }
        let output_device_table = Table::new(output_device_rows, widths)
            .header(
                Row::new(vec!["device name", "channels", "sample rate"])
                    .style(Style::new().bold())
                    .bottom_margin(1),
            )
            .block(
                Block::bordered()
                    .title("select output device")
                    .border_style({
                        if self.selected_selection == 1 {
                            Style::new().bold()
                        } else {
                            Style::default()
                        }
                    })
                    .border_type({
                        if self.selected_selection == 1 {
                            ratatui::widgets::BorderType::Thick
                        } else {
                            ratatui::widgets::BorderType::Plain
                        }
                    }),
            )
            .row_highlight_style(Style::new().reversed());

        frame.render_stateful_widget(
            output_device_table,
            right_block,
            &mut self.output_device_state,
        );
    }

    fn handle_events(&mut self) {
        match event::read().expect("failed to read event") {
            Event::Key(key_event) if key_event.kind == KeyEventKind::Press => {
                self.handle_key_events(key_event);
            }
            _ => {}
        }
    }

    fn handle_key_events(&mut self, key_event: KeyEvent) {
        match key_event.code {
            KeyCode::Char('q') => self.exit(),
            KeyCode::Down => self.increment_table(),
            KeyCode::Up => self.decrement_table(),
            KeyCode::Left => self.selected_selection = 0,
            KeyCode::Right => self.selected_selection = 1,
            // KeyCode::Down => self.increment_list(),
            // KeyCode::Enter => self.get_value(),
            _ => {}
        }
    }

    fn exit(&mut self) {
        self.exit = true;
    }

    fn increment_table(&mut self) {
        if self.selected_selection == 0 {
            self.input_device_state.scroll_down_by(1);
        } else {
            self.output_device_state.scroll_down_by(1);
        }
    }

    fn decrement_table(&mut self) {
        if self.selected_selection == 0 {
            self.input_device_state.scroll_up_by(1);
        } else {
            self.output_device_state.scroll_up_by(1);
        }
    }

    // fn get_value(&mut self) {
    //     self.table_state.selected();
    // }

    // fn increment_list(&mut self) {
    //     self.list_state.scroll_down_by(1);
    //     self.table_state.scroll_down_by(1);
    // }

    // fn decrement_list(&mut self) {
    //     self.list_state.scroll_up_by(1);
    //     self.table_state.scroll_up_by(1);
    // }
}
