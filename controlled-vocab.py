import json
import sys
import threading
import time


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
    print('    -c, --case-sensitive     Should the conversion be case sensitive (defaults\n                             is False).')
    print('    -f, --fuse               Fuse the results into a single file.')
    print('    -h, --help               Shows this help message.')
    print('    -i, --input              List of input files.')
    print('    -m, --mapping            File with a (initial) mapping to use.')
    print('    -o, --output             List of output files for each input file, followed\n                             by a file to export the mapping (JSON format).')


# Configuration
def parse_args(args):
    """Parse the configuration arguments"""
    global global_mapping, mapping_flag
    config = {
        'case_sensitive': False,
        'input_files':    [],
        'mapping_input' : None,
        'mapping_output': None,
        'merge':          False,
        'output_files':   []
    }

    current_key = None
    for arg in args:
        if arg[0:1] == '-':
            if arg == '--case-sensitive' or arg == '-c':
                current_key = 'case_sensitive'
                config['case_sensitive'] = True
            elif arg == '--fuse' or arg == '-f':
                current_key = 'merge'
                config['merge'] = True
            elif arg == '--help' or arg == '-h':
                show_help()
                exit(0)
            elif arg == '--input' or arg == '-i':
                current_key = 'input'
            elif arg == '--mapping' or arg == '-m':
                current_key = 'mapping_input'
            elif arg == '--output' or arg == '-o':
                current_key = 'output'
        else:
            if current_key == 'input':
                config['input_files'].append(arg)
            if current_key == 'mapping_input':
                mapping_file = open(arg, 'r')
                global_mapping = json.load(mapping_file)
                mapping_flag = True
            elif current_key == 'output':
                if len(config['output_files']) < len(config['input_files']) or (len(config['output_files']) > 0 and config['merge'] is True):
                    config['output_files'].append(arg)
                else:
                    config['mapping_output'] = arg

    return config


# Thread classes
class ReadThread(threading.Thread):
    def __init__(self, input_files, output_files, merge):
        super(ReadThread, self).__init__()

        if len(input_files) == 0:
            show_help()
            print('\nNo input files found...\n')
            exit(0)

        self.input_files = input_files
        self.local_values = []
        self.merge = merge
        self.output_files = output_files

    ### Thread
    def run(self):
        global finished_reading
        total_line_counter = 0

        # Read the content of each input file
        for i, file_path in enumerate(self.input_files):
            file = open(file_path, 'r')
            for number, value in enumerate(file):
                line = {
                    'file': self.output_files[i] if len(self.output_files) > i else None,
                    'line_nr': number if self.merge is False else total_line_counter,
                    'value': value[:-1]
                }

                self.local_values.append(line)
                total_line_counter += 1

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
                    waiting = self.local_waiting[i]
                    if self.convert_value(waiting, prompt=False):
                        self.local_waiting.pop(i)

            for input in self.local_input:
                self.convert_value(input, prompt=True)

            self.publish()

        finished_converting = True

    ### ConvertThread
    def convert_value(self, input, prompt):
        """Convert a value into another value based on the known (local) mapping"""
        value = input['value']
        key = value
        if self.case_sensitive is False:
            key = value.lower()

        # If the key is mapped add to result, otherwise prompt user
        if key in self.local_mapping:
            input['value'] = self.local_mapping[key]
            self.local_output.append(input)
            return True
        elif prompt is True:
            self.prompt(input)

        return False

    def get_values(self):
        """Read the values published to global_input"""
        global input_semaphore, global_input

        input_semaphore.acquire()
        self.local_input = global_input
        global_input = [] # Clear published values as to not process them twice
        input_semaphore.release()

    def prompt(self, input):
        """Promp the user to give a mapping"""
        global prompt_semaphore, global_prompt

        value = input['value']
        key = value
        if self.case_sensitive is False:
            key = value.lower()

        prompt_semaphore.acquire()
        if not value in global_prompt and not key in global_prompt:
            global_prompt.append(value)
        prompt_semaphore.release()

        self.local_waiting.append(input)

    def publish(self):
        """Publish the converted values to the global output"""
        global output_semaphore, global_output

        if len(self.local_output) > 0:
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
    def __init__(self, output_files, mapping_file, merge):
        super(WriteThread, self).__init__()

        self.current_ln = {} if output_files is not None else None
        self.local_values = []
        self.mapping_file = open(mapping_file, 'w+') if mapping_file is not None else None
        self.output_file = output_files[0] if merge is True else None
        self.output_files = {} if output_files is not None else None

        for file in output_files:
            self.output_files[file] = open(file, 'w+')
            self.current_ln[file] = 0

    ### Thread
    def run(self):
        global finished_converting
        while True:
            self.get_values()

            if self.has_values():
                self.write_results()
            else:
                time.sleep(0.5)

            if finished_converting is True and not self.has_values():
                break

        if self.output_files is not None:
            for _, file in self.output_files.items():
                file.close()
        else:
            self.print_output()

        if self.mapping_file is not None:
            self.write_mapping()

    ### WriteThread
    def get_values(self):
        """Read the values published to global_input"""
        global output_semaphore, global_output

        output_semaphore.acquire()
        self.local_values = self.local_values + global_output
        global_output = [] # Clear published values as to not process them twice
        output_semaphore.release()

    def has_values(self):
        return len(self.local_values) > 0

    def print_output(self):
        """Print the results to the command line"""
        print('')
        self.print_mapping()
        self.print_percentages()

    def write_results(self):
        """Write the results to the output file"""
        self.local_values = sorted(self.local_values, key=lambda k: k['line_nr'], reverse=True)
        i = len(self.local_values)
        while i > 0:
            i -= 1
            line = self.local_values[i]

            # Check if this is the next line in its respective file
            file_name = line['file'] if self.output_file is None else self.output_file
            if self.current_ln[file_name] == line['line_nr']:
                output_file = self.output_files[file_name]
                output_file.write(line['value'] + '\n')

                # Remove the value from memory and increment the line counter
                self.local_values.pop(i)
                self.current_ln[file_name] += 1

    def write_mapping(self):
        """Write the mapping to the export file"""
        if self.mapping_file is not None:
            json_mapping = json.dumps(global_mapping)
            self.mapping_file.write(json_mapping)
            self.mapping_file.close()

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


def main():
    config = parse_args(sys.argv)

    reader = ReadThread(config['input_files'], config['output_files'], config['merge'])
    reader.start()

    converter = ConvertThread(config['case_sensitive'])
    converter.start()

    prompter = PromptThread(config['case_sensitive'])
    prompter.start()

    writer = WriteThread(config['output_files'], config['mapping_output'], config['merge'])
    writer.start()

if __name__ == '__main__':
    main()
