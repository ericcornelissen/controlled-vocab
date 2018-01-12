import sys
import threading
import time

# Miscellaneous
def show_help():
    """Log the --help command"""
    print('Welcome to Controlled Vocabulary Tool. A Python3 CLI tool by Eric Cornelissen.')
    print('This tool can be used to normalize the vocabulary of raw data and is effictive')
    print('on large quantities of data.')
    print('')
    print('Tips:')
    print('    - If you don\'t type a value and press ENTER, the output value will be the\n      same as the input value.')
    print('')
    print('Arguments:')
    print('    -c, --case-sensitive     Should the conversion be case sensitive (defaults is False)')
    print('    -f, --file               List of input files')
    print('    -h, --help               Shows this help message')
    print('    -o, --output             File to write the result to')


# Configuration
def parse_args(args):
    """Parse the configuration arguments"""
    config = {
        'input': [],
        'output': None,
        'case_sensitive': False
    }

    current_key = None
    for arg in args:
        if arg[0:1] == '-':
            if arg == '--case-sensitive' or arg == '-c':
                current_key = 'case_sensitive'
            elif arg == '--file' or arg == '-f':
                current_key = 'input'
            elif arg == '--help' or arg == '-h':
                show_help()
                exit(0)
            elif arg == '--output' or arg == '-o':
                current_key = 'output'
            else:
                current_key = None
        else:
            if current_key == 'case_sensitive':
                config['case_sensitive'] = True if arg.lower() == 'true' else False
            elif current_key == 'input':
                config['input'].append(arg)
            elif current_key == 'output':
                config['output'] = arg

    return config


# Thread variables
global_input = []
input_semaphore = threading.Semaphore(1)

global_output = []
output_semaphore = threading.Semaphore(1)

global_prompt = []
prompt_semaphore = threading.Semaphore(1)

global_mapping = {}
mapping_flag = False
mapping_semaphore = threading.Semaphore(1)

finished_reading = False
finished_converting = False


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
        self.local_mapping = {}
        self.local_output = []
        self.local_waiting = []

    ### Thread
    def run(self):
        global finished_converting, finished_reading
        while True:
            self.get_values()
            if finished_reading and len(self.local_input) == 0 and len(self.local_waiting) == 0:
                break

            if self.update_mapping() is True:
                # Only try to convert waiting values when the map has been
                # updated, otherwise it would have been possible to convert
                # them immediately.
                # Go through in reverse order so converted elements can be
                # removed safely.
                i = len(self.local_waiting)
                while i > 0:
                    i -= 1
                    value = self.local_waiting[i]
                    if self.convert_value(value, prompt=False):
                        self.local_waiting.pop(i)

            for value in self.local_input:
                self.convert_value(value, prompt=True)

            self.publish()

        finished_converting = True

    ### ConvertThread
    def convert_value(self, value, prompt):
        """Convert a value into another value based on the known (local) mapping"""
        key = value
        if self.case_sensitive is False:
            key = value.lower()

        # If the key is mapped add to result, otherwise prompt user
        if key in self.local_mapping:
            self.local_output.append(self.local_mapping[key])
            return True
        elif prompt is True:
            self.prompt(value)

        return False

    def get_values(self):
        """Read the values published to global_input"""
        global input_semaphore, global_input

        input_semaphore.acquire()
        self.local_input = global_input
        global_input = [] # Clear published values as to not process them twice
        input_semaphore.release()

    def prompt(self, value):
        """Promp the user to give a mapping"""
        global prompt_semaphore, global_prompt

        key = value
        if self.case_sensitive is False:
            key = value.lower()

        prompt_semaphore.acquire()
        if not value in global_prompt and not key in global_prompt:
            global_prompt.append(value)
        prompt_semaphore.release()

        self.local_waiting.append(value)

    def publish(self):
        """Publish the converted values to the global output"""
        global output_semaphore, global_output

        output_semaphore.acquire()
        global_output = global_output + self.local_output
        output_semaphore.release()

        self.local_output = []

    def update_mapping(self):
        """Synchronize the local mapping with the global mapping"""
        global mapping_semaphore, global_mapping, mapping_flag
        updated = False

        mapping_semaphore.acquire()
        if mapping_flag is True:
            self.local_mapping = global_mapping
            mapping_flag = False
            updated = True
        mapping_semaphore.release()

        return updated

class PromptThread(threading.Thread):
    def __init__(self, case_sensitive):
        super(PromptThread, self).__init__()

        self.case_sensitive = case_sensitive
        self.prompt_index = 0

    def run(self):
        global finished_converting
        while True:
            if finished_converting is True:
                break

            value = self.get_prompt()
            if value is not None:
                mapped_value = input(f'What should "{value}" be mapped to? ')

                if mapped_value == '':
                    mapped_value = value

                self.publish(value, mapped_value)
            else:
                time.sleep(0.5)

    ### PromptThread
    def get_prompt(self):
        """Get a new prompt from the global prompt list"""
        global prompt_semaphore, global_prompt
        value = None

        prompt_semaphore.acquire()
        if len(global_prompt) > self.prompt_index:
            value = global_prompt[self.prompt_index]
            self.prompt_index += 1
        prompt_semaphore.release()

        return value

    def publish(self, value, mapped_value):
        """Publish a new mapping to the globalmapping"""
        global mapping_semaphore, global_mapping, mapping_flag

        key = value
        if not self.case_sensitive:
            key = value.lower()

        mapping_semaphore.acquire()
        global_mapping[key] = mapped_value
        mapping_flag = True
        mapping_semaphore.release()

class WriteThread(threading.Thread):
    def __init__(self, file):
        super(WriteThread, self).__init__()

        self.file = open(file, 'w+') if file is not None else None
        self.local_values = []

    ### Thread
    def run(self):
        global finished_converting
        while True:
            self.get_values()

            if self.file is not None:
                self.write_results()
            else:
                time.sleep(0.5)

            if finished_converting:
                break

        if self.file is not None:
            self.file.close()
        else:
            self.print_results()

    ### ConvertThread
    def get_values(self):
        """Read the values published to global_input"""
        global output_semaphore, global_output

        output_semaphore.acquire()
        self.local_values = self.local_values + global_output
        global_output = [] # Clear published values as to not process them twice
        output_semaphore.release()

    def print_results(self):
        """Print the results to the command line"""
        print('')
        self.print_mapping()
        self.print_percentages()

    def write_results(self):
        """Write the results to the output file"""
        for value in self.local_values:
            self.file.write(value + '\n')
        self.local_values = []

    ### Utility
    def print_mapping(self):
        """Print the mapping"""
        self.print_title('mapping')
        print(global_mapping)

    def print_percentages(self):
        """Print the percentages of each value in the results"""
        total = len(self.local_values)

        # Get a list of unique output values from the mapping
        unique_values = []
        for _, value in global_mapping.items():
            if not value in unique_values:
                unique_values.append(value)

        # Log the percentage for each unique value
        self.print_title('percentages')
        for value in unique_values:
            count = self.local_values.count(value)
            percentage = (count / total) * 100
            print(f'{value}: {percentage}%')

    def print_title(self, title):
        """Print a title"""
        print(f'\n{title.upper()}\n-----------------')


# Main
config = parse_args(sys.argv)

reader = ReadThread(config['input'])
reader.start()

converter = ConvertThread(config['case_sensitive'])
converter.start()

prompter = PromptThread(config['case_sensitive'])
prompter.start()

writer = WriteThread(config['output'])
writer.start()
