import sys
import threading
import time

# Miscellaneous
def show_help():
    """Log the --help command"""
    print('Welcome to...\n')
    print('    -cs, --case_sensitive    Should the conversion be case sensitive (defaults is False)')
    print('    -f, --file               List of input files')
    print('    -h, --help               Shows this help message')


# Configuration
def parse_args(args):
    """Parse the configuration arguments"""
    config = {
        'input': [],
        'case_sensitive': False
    }

    current_key = None
    for arg in args:
        if arg[0:1] == '-':
            if arg == '--help' or arg == '-h':
                show_help()
                exit(0)
            elif arg == '--file' or arg == '-f':
                current_key = 'input'
            elif arg == '--case-sensitive' or arg == '-cs':
                current_key = 'case_sensitive'
            else:
                current_key = None
        else:
            if current_key == 'input':
                config[current_key].append(arg)
            elif current_key == 'case_sensitive':
                config[current_key] = True if arg.lower() == 'true' else False

    return config


# Thread variables
global_input = []
input_semaphore = threading.Semaphore(1)

global_output = []
output_semaphore = threading.Semaphore(1)

finished_reading = False
finished_converting = False

mapping = {}


# Thread classes
class ReadThread(threading.Thread):
    def __init__(self, files):
        super(ReadThread, self).__init__()
        global finished_reading

        if len(files) == 0:
            finished_reading = True # Stops the ConvertThread
            raise RuntimeError('no input')

        self.files = files
        self.local_values = []

    ### Thread
    def run(self):
        global finished_reading

        # Read the content of each input file
        for file_path in self.files:
            file = open(file_path, 'r')
            for line in file:
                value = line[:-1]
                self.local_values.append(value)

                # Publish the local values every 50000 lines
                if len(self.local_values) > 50000:
                    self.publish()

            # Publish any remaining values from this file
            self.publish()

        finished_reading = True

    ### ReadThread
    def publish(self):
        """Publish the values read to global_input"""
        global input_semaphore, global_input

        input_semaphore.acquire()
        global_input = global_input + self.local_values
        input_semaphore.release()

        self.local_values = []

class ConvertThread(threading.Thread):
    def __init__(self, case_sensitive):
        super(ConvertThread, self).__init__()

        self.case_sensitive = case_sensitive
        self.local_input = []
        self.local_output = []

    ### Thread
    def run(self):
        global finished_converting, finished_reading, mapping
        while True:
            self.get_values()
            if finished_reading and len(self.local_input) == 0:
                break

            for value in self.local_input:
                key = value
                if not self.case_sensitive:
                    key = value.lower()

                # If the key is mapped, add to result, otherwise prompt user
                if key in mapping:
                    self.local_output.append(mapping[key])
                else:
                    mapped_value = input(f'What should "{value}" be mapped to? ')

                    # Default to input if no value is provided
                    if mapped_value == '':
                        mapped_value = value

                    # Remember the mapping and update the result
                    mapping[key] = mapped_value
                    self.local_output.append(mapped_value)

            self.publish()

        finished_converting = True

    ### ConvertThread
    def get_values(self):
        """Read the values published to global_input"""
        global input_semaphore, global_input

        input_semaphore.acquire()
        self.local_input = global_input
        global_input = [] # Clear published values as to not process them twice
        input_semaphore.release()

    def publish(self):
        """Publish the converted values to the global output"""
        global output_semaphore, global_output

        output_semaphore.acquire()
        global_output = global_output + self.local_output
        output_semaphore.release()

        self.local_output = []

class WriteThread(threading.Thread):
    def __init__(self):
        super(WriteThread, self).__init__()

        self.local_values = []

    ### Thread
    def run(self):
        global finished_converting
        while True:
            self.get_values()
            if finished_converting:
                break

            time.sleep(1)

        self.log_results()
        self.log_percentages()

    ### ConvertThread
    def get_values(self):
        """Read the values published to global_input"""
        global output_semaphore, global_output

        output_semaphore.acquire()
        self.local_values = self.local_values + global_output
        global_output = [] # Clear published values as to not process them twice
        output_semaphore.release()

    def log_percentages(self):
        """Log the percentages of each value in the result"""
        total = len(self.local_values)

        # Get a list of unique output values from the mapping
        unique_values = []
        for _, value in mapping.items():
            if not value in unique_values:
                unique_values.append(value)

        # Log the percentage for each unique value
        self.log_title('percentages')
        for value in unique_values:
            count = self.local_values.count(value)
            percentage = (count / total) * 100
            print(f'{value}: {percentage}%')

    def log_results(self):
        self.log_title('output')
        print(self.local_values)
        self.log_title('mapping')
        print(mapping)

    ### Utility
    def log_title(self, title):
        print(f'\n\n{title.upper()}\n-----------------')


# Main
config = parse_args(sys.argv)

reader = ReadThread(config['input'])
reader.start()

converter = ConvertThread(config['case_sensitive'])
converter.start()

writer = WriteThread()
writer.start()
