def print_hash(count, change_line = False):
    if change_line:
        end = '\n'
    else:
        end = ''
    print('#' * count, end = end)

def print_space(count, change_line = False):
    if change_line:
        end = '\n'
    else:
        end = ''
    print(' ' * count, end = end)


def print_full_line(header_width):
    print_hash(header_width, change_line = True)

def print_border_line(header_width):
    print_hash(1, change_line = False)
    print_space(header_width -2, change_line= False)
    print_hash(1, change_line = True)

def print_text_line(header_width, header_text):
    print_hash(1, change_line = False)

    leading_space = ((header_width-2-len(header_text))//2)
    if leading_space == ((header_width-2-len(header_text))/2):
        trailing_space =  leading_space
    else:
        trailing_space = leading_space + 1

    print_space(leading_space, change_line= False)

    print(header_text, end = '') 

    print_space(trailing_space, change_line= False)
    print_hash(1, change_line = True)


def print_header_box(header_text, box_width):

    # first/full line
    print_full_line(box_width)

    # second/border line
    print_border_line(box_width)

    # third/text line 
    print_text_line(box_width, header_text)

    # fourth/border line
    print_border_line(box_width)

    # fifth/full line
    print_full_line(box_width)
