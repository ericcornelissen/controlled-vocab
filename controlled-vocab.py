import sys

divider_str = '-------------------------'
config = {
    'input': [],
    'case_sensitive': False
}

def show_help():
    print("Lorem ipsum dolor\n")

def parse_config(args):
    """TODO: add documentation"""
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

def read_file(file_path):
    """TODO: add documentation"""
    values = []
    f = open(file_path, "r")
    for line in f:
        values.append(line[:-1])
    return values

def map_it(files, case_sensitive=False):
    """TODO: add documentation"""
    mapping = {}
    output = []

    # Make sure there is some input
    if len(files) == 0:
        raise RuntimeError('no input')

    # Collect the input data
    items = []
    for file in files:
        local_items = read_file(file)
        items = items + local_items

    # Map and convert the input data
    for item in items:
        # Use lower case internally if not case sensitive
        key = item
        if not case_sensitive:
            key = item.lower()

        # If the key is mapped, add to result, otherwise prompt user
        if key in mapping:
            output.append(mapping[key])
        else:
            value = input('What should "%s" be mapped to? ' % item)
            
            # Default to input if no value is provided
            if value == '':
                value = item
            
            # Remember the mapping and update the result
            mapping[key] = value
            output.append(value)
    
    return output, mapping

def show_results(output, mapping):
    print('\n\nOUTPUT\n%s' % divider_str)
    print(output)
    print('\nMAPPING\n%s' % divider_str)
    print(mapping)
    
def show_percenages(output, mapping):
    unique_values = []
    for _, value in mapping.items():
        if not value in unique_values:
            unique_values.append(value)

    print('\n\nPERCENTAGES\n%s' % divider_str)
    for value in unique_values:
        total = len(output)
        fraction = output.count(value)
        percentage = (fraction / total) * 100
        print(f'{value} ({fraction}/{total}): {percentage}%')


parse_config(sys.argv)
output, mapping = map_it(config['input'], config['case_sensitive'])

show_results(output, mapping)
show_percenages(output, mapping)
