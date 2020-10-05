# To generate executable:
#   pyinstaller --add-data OutputExcelTemplate_v09_20200922.xlsm;. --onedir NoiseDataProcessor_ui.py

import sys
from PyQt5.QtWidgets import QApplication, QMainWindow, QPushButton, QVBoxLayout, QWidget, QFileDialog, QMessageBox
from PyQt5.QtCore import QCoreApplication
import infer_filetype
from read_data import read
from process import *
import outputs_ui
from os import startfile
from json import dumps

__version__ = "0.1"
__author__ = "Peter Ling"


# Create a subclass of QMainWindow to setup the app's GUI
class NoiseDataProcessorUi(QMainWindow):
    """App's View (GUI)."""

    def __init__(self):
        """View initializer."""
        super().__init__()
        # Set some main window's properties
        self.setWindowTitle("Noise Data Processor")
        self.setFixedSize(300, 150)
        # Set the central widget and the general layout
        self.generalLayout = QVBoxLayout()
        self._centralWidget = QWidget(self)
        self.setCentralWidget(self._centralWidget)
        self._centralWidget.setLayout(self.generalLayout)
        # Create the buttons
        self._create_buttons()

    def _create_buttons(self):
        """Create the buttons."""
        self.buttons = {}
        buttons_layout = QVBoxLayout()
        # Button text | position on the QGridLayout
        buttons = {
            "Select config file...": 0,
            "Select/override input file...": 1,
            "Select/override output file...": 2,
            "Process data": 3,
        }
        # Create the buttons and add them to the grid layout
        for btnText, pos in buttons.items():
            self.buttons[btnText] = QPushButton(btnText)
            buttons_layout.addWidget(self.buttons[btnText], pos)
        # Add buttons_layout to the general layout
        self.generalLayout.addLayout(buttons_layout)


# Create a Controller class to connect the GUI and the model
class NoiseDataProcessorCtrl:
    """Noise Data Processor's Controller."""

    def __init__(self, config, view):
        """Controller initializer."""
        self._config = config
        self._view = view
        # Connect signals and slots
        self._connect_signals()

    def _input_select(self):
        """Open file browser to select input data file"""
        options = QFileDialog.Options()
        file_select, _ = QFileDialog.getOpenFileNames(
            self._view,
            'Select Input File...',
            '',
            'RNH Files (*.rnh);;RND Files (*.rnd);;CSV Files (*.csv);;Excel Files (*.xls*);;All Files (*)',
            options=options
        )
        if file_select:
            self._config["input"] = file_select

    def _config_select(self):
        """Open file browser to select config file"""
        options = QFileDialog.Options()
        file_select, _ = QFileDialog.getOpenFileNames(
            self._view,
            'Select Config File...',
            '',
            'Text Files (*.txt);;All Files (*)',
            options=options
        )
        if file_select:
            config_raw = eval(open(file_select[0], 'r').read())
            self._config.update(config_raw)

    def _output_select(self):
        """Open file browser to set output file"""
        options = QFileDialog.Options()
        file_select, _ = QFileDialog.getSaveFileName(
            self._view,
            'Save Output...',
            '',
            # 'RNH Files (*.rnh);;RND Files (*.rnd);;CSV Files (*.csv);;Excel Files (*.xls*);;All Files (*)',
            options=options
        )
        if file_select:
            self._config["output"] = [file_select]

    def _process_data(self):

        # Read config file
        config = self._config

        if "input" not in config.keys():
            self._no_input_dialog()
            return

        if "output" not in config.keys():
            self._no_output_dialog()
            return

        fields = [
            'type',
            'modules',
            'lmax summary remove',
        ]

        for f in fields:
            if f not in config.keys():
                self._incomplete_config_dialog(f)
                return

        # Infer file type if config set to auto
        if config["type"] == "auto":
            file_type = infer_filetype.infer(config["input"][0])
            print("\nInferred file type: " + file_type)
        else:
            file_type = config["type"]

        # Read input data
        print("Reading data...")
        user_metadata = config["percentiles"].copy()
        user_metadata.insert(0, config["frequency weighting"])

        if 'columns' in config.keys():
            data, metadata = read(file_type, config["input"], user_metadata, columns=config["columns"])
        else:
            data, metadata = read(file_type, config["input"], user_metadata)

        print("Data read successfully")

        # Run pre-processing modules
        data, _ = process_batch(data, config["modules"], metadata)

        # Generate output tables
        tables = outputs_ui.daily_table(
            data,
            metadata["Frequency Weighting"],
            config["lmax summary remove"],
            config["lmax summary override"]
        )

        # Export to Excel (also export config duplicate)
        writer, config_out = outputs_ui.export_excel(data, metadata, tables, config)

        while True:
            try:

                # Export workbook
                writer.save()

                # Export config file
                with open(config_out, 'w') as file:
                    file.write(dumps(config, indent=4))

                print("Export complete")

                startfile(config["output"][0] + ".xlsm")
                QCoreApplication.quit()
                break

            except PermissionError:
                action = self._file_open_dialog()
                if action == 4194304:
                    QCoreApplication.quit()
                    break

    def _connect_signals(self):
        """Connect signals and slots."""
        self._view.buttons["Select config file..."].clicked.connect(self._config_select)
        self._view.buttons["Select/override input file..."].clicked.connect(self._input_select)
        self._view.buttons["Select/override output file..."].clicked.connect(self._output_select)
        self._view.buttons["Process data"].clicked.connect(self._process_data)

    def _no_input_dialog(self):
        msg = QMessageBox()
        msg.setWindowTitle("No Input")
        msg.setText("No input file selected")
        msg.setIcon(QMessageBox.Critical)
        msg.exec_()

    def _no_output_dialog(self):
        msg = QMessageBox()
        msg.setWindowTitle("No Output")
        msg.setText("No output file selected")
        msg.setIcon(QMessageBox.Critical)
        msg.exec_()

    def _incomplete_config_dialog(self, field):
        msg = QMessageBox()
        msg.setWindowTitle("Incomplete config")
        msg.setText("Config file is missing " + field)
        msg.setIcon(QMessageBox.Critical)
        msg.exec_()

    def _file_open_dialog(self):
        msg = QMessageBox()
        msg.setWindowTitle("File open")
        msg.setText("Please close output file to overwrite")
        msg.setIcon(QMessageBox.Critical)
        msg.setStandardButtons(QMessageBox.Retry | QMessageBox.Cancel)
        msg.setDefaultButton(QMessageBox.Retry)
        msg.buttonClicked.connect(self._popup_clicked)
        action = msg.exec_()
        return action

    def _popup_clicked(self, action):
        return action.text()


# Client code
def main():
    """Main function."""
    # Create an instance of `QApplication`
    ndp = QApplication(sys.argv)
    # Show the calculator's GUI
    view = NoiseDataProcessorUi()
    view.show()
    # Create instances of the model and the controller
    config = {}
    ctrl = NoiseDataProcessorCtrl(config=config, view=view)
    # Execute calculator's main loop
    sys.exit(ndp.exec_())


if __name__ == "__main__":
    main()
