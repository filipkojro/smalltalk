use cpal::{
    Device, SupportedStreamConfig,
    traits::{DeviceTrait, HostTrait},
};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind};
use gag::Gag;
use ratatui::{
    DefaultTerminal, Frame,
    layout::{Constraint, Direction, Layout},
    style::Style,
    widgets::{Block, Paragraph, Row, Table, TableState},
};
use std::{
    sync::{Arc, RwLock},
    vec,
};
use tokio::sync::broadcast::Sender;

use crate::{
    audio_streams::{audio_stream_receiver, audio_stream_spawner},
    models::{AudioControl, ClientEvent, ClientInfo, TuiScreen},
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
        audio_control: Arc<AudioControl>,
        global_event_tx: Sender<ClientEvent>,
        client_info: Arc<RwLock<ClientInfo>>,
    ) {
        loop {
            match self.current_screen {
                TuiScreen::Exit => {
                    break;
                }
                TuiScreen::Blank => self.current_screen = BlankScreen::new().run(terminal),
                TuiScreen::DeviceSelect => {
                    self.current_screen = DeviceSelectScreen::new(
                        audio_control.clone(),
                        global_event_tx.clone(),
                        client_info.clone(),
                    )
                    .run(terminal);
                }
            }
        }
    }
}

struct BlankScreen {
    exit: bool,
}

impl BlankScreen {
    pub fn new() -> Self {
        Self { exit: false }
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
        frame.render_widget(Paragraph::new("q - quit"), frame.area());
    }

    fn handle_events(&mut self) {
        match event::read().expect("failed to read event") {
            Event::Key(key_event) if key_event.kind == KeyEventKind::Press => {
                match key_event.code {
                    KeyCode::Char('q') => self.exit = true,
                    _ => {}
                }
            }
            _ => {}
        }
    }
}

struct DeviceSelectScreen {
    exit: bool,
    exit_screen: TuiScreen,
    input_device_state: TableState,
    output_device_state: TableState,
    input_devices: Vec<(Device, SupportedStreamConfig)>,
    output_devices: Vec<(Device, SupportedStreamConfig)>,
    selected_selection: u8,

    audio_control: Arc<AudioControl>,
    global_event_tx: Sender<ClientEvent>,
    client_info: Arc<RwLock<ClientInfo>>,
}

impl DeviceSelectScreen {
    pub fn new(
        audio_control: Arc<AudioControl>,
        global_event_tx: Sender<ClientEvent>,
        client_info: Arc<RwLock<ClientInfo>>,
    ) -> Self {
        let _print_gag = Gag::stderr().ok();

        let host = cpal::default_host();

        let mut input_devices: Vec<(Device, SupportedStreamConfig)> = Vec::new();

        if let Some(default_input_device) = host.default_input_device() {
            if let Ok(default_config) = default_input_device.default_input_config() {
                input_devices.push((default_input_device.clone(), default_config));
            }
        }

        if let Ok(possible_input_devices) = host.input_devices() {
            for possible_input_device in possible_input_devices {
                if let Ok(default_config) = possible_input_device.default_input_config() {
                    input_devices.push((possible_input_device.clone(), default_config));
                }
            }
        }

        let mut output_devices: Vec<(Device, SupportedStreamConfig)> = Vec::new();

        if let Some(default_output_device) = host.default_output_device() {
            if let Ok(default_config) = default_output_device.default_output_config() {
                output_devices.push((default_output_device.clone(), default_config.into()));
            }
        }

        if let Ok(possible_output_devices) = host.output_devices() {
            for possible_output_device in possible_output_devices {
                if let Ok(default_config) = possible_output_device.default_output_config() {
                    output_devices.push((possible_output_device.clone(), default_config.into()));
                }
            }
        }

        let mut input_state = TableState::default();
        input_state.select(Some(0));

        let mut output_state = TableState::default();
        output_state.select(Some(0));

        Self {
            exit: false,
            exit_screen: TuiScreen::Exit,
            input_device_state: input_state,
            output_device_state: output_state,
            input_devices: input_devices,
            output_devices: output_devices,
            selected_selection: 0,

            audio_control: audio_control,
            global_event_tx: global_event_tx,
            client_info: client_info,
        }
    }

    pub fn run(&mut self, terminal: &mut DefaultTerminal) -> TuiScreen {
        while !self.exit {
            terminal
                .draw(|frame| self.draw(frame))
                .expect("failed to draw");
            self.handle_events();
        }

        return self.exit_screen;
    }

    fn draw(&mut self, frame: &mut Frame) {
        let outer_layout = Layout::default()
            .direction(Direction::Horizontal)
            .constraints(vec![Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(frame.area());

        let block = Block::new();
        let left_block = block.inner(outer_layout[0]);
        let right_block = block.inner(outer_layout[1]);

        let widths = [Constraint::Percentage(100), Constraint::Min(8)];

        let mut input_device_rows: Vec<Row> = Vec::new();
        for (i, (device, config)) in self.input_devices.iter().enumerate() {
            if i == 0 {
                input_device_rows.push(Row::new(vec![
                    format!("(default) {}", device.description().unwrap().name()),
                    config.channels().to_string(),
                ]));
            } else {
                input_device_rows.push(Row::new(vec![
                    device.description().unwrap().name().to_string(),
                    config.channels().to_string(),
                ]));
            }
        }
        let input_device_table = Table::new(input_device_rows, widths)
            .header(
                Row::new(vec!["device name", "channels"])
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
        for (i, (device, config)) in self.output_devices.iter().enumerate() {
            if i == 0 {
                output_device_rows.push(Row::new(vec![
                    format!(
                        "(default) {}",
                        device.description().unwrap().name().to_string()
                    ),
                    config.channels().to_string(),
                ]));
            } else {
                output_device_rows.push(Row::new(vec![
                    device.description().unwrap().name().to_string(),
                    config.channels().to_string(),
                ]));
            }
        }
        let output_device_table = Table::new(output_device_rows, widths)
            .header(
                Row::new(vec!["device name", "channels"])
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
            KeyCode::Enter => self.connect_devices(),
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

    fn connect_devices(&mut self) {
        if let (Some(input_device_row), Some(output_device_row)) = (
            self.input_device_state.selected(),
            self.output_device_state.selected(),
        ) {
            let (input_device, input_config) = self
                .input_devices
                .get(input_device_row)
                .expect("failed to get input config");
            let (output_device, output_config) = self
                .output_devices
                .get(output_device_row)
                .expect("failed to get output config");

            tokio::spawn(audio_stream_receiver(
                output_device.clone(),
                output_config.clone(),
                self.audio_control.active_input_audio_streams_info.clone(),
                self.audio_control.udp_socket.clone(),
            ));

            tokio::spawn(audio_stream_spawner(
                input_device.clone(),
                input_config.clone(),
                self.global_event_tx.clone(),
                self.global_event_tx.subscribe(),
                self.audio_control.udp_socket.clone(),
                self.client_info.clone(),
                1,
            ));

            self.exit_screen = TuiScreen::Blank;
            self.exit = true;
        }
    }
}
