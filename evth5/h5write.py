from . import nscl_convert
from . import faster_convert
import socket
import os
import tables as tb

class DetectorHit(tb.IsDescription):
    crate       = tb.Int32Col()
    slot        = tb.Int32Col()
    channel     = tb.Int32Col()
    energy      = tb.Int64Col()
    time        = tb.Float64Col()
    time_raw    = tb.Float64Col()
    is_trace    = tb.BoolCol()
    trace_idx   = tb.Int32Col()
    qdc_0       = tb.Int32Col()
    qdc_1       = tb.Int32Col()
    qdc_2       = tb.Int32Col()
    qdc_3       = tb.Int32Col()
    qdc_4       = tb.Int32Col()
    qdc_5       = tb.Int32Col()
    qdc_6       = tb.Int32Col()
    qdc_7       = tb.Int32Col()


def frib_paths(exp_num):
    """
    Return the path to the events depending on which network
    you are on.
    """
    
    if 'e' in exp_num:
        exp_string =  exp_num + '/experiment/'
    else:
        exp_string =  'e' + exp_num + '/experiment/'
        
    host = socket.gethostname()
    
    fishtank_path = '/mnt/rawdata/' + exp_string
    daq_path = '/events/' + exp_string

    if host == 'pike' or host == 'steelhead' or host == 'flagtail':
        return fishtank_path
    else:
        print('Host does not appear to be fishtank, probably not a good place to do conversion')
        return daq_path
    


def convert_run(exp_num, evt_run_number,
                h5_filename=None, h5_path='',
                event_chunk_size=100000, complevel=3):

    """
    def convert_run(exp_num, evt_run_number,
                    h5_filename=None, h5_path='',
                    event_chunk_size=100000, complevel=3):
    
    

    Reads all segments of a run and convert to a single h5 file.

    """

    exp_num = str(exp_num)
    evt_run_number = str(evt_run_number)
    
    # get the names of all the run segments
    exp_path = frib_paths(exp_num)

    run_string = 'run'+evt_run_number

    try:
        all_files = os.listdir(exp_path + run_string)

    except FileNotFoundError:
        print(run_string + ' not found in directory: ' + exp_path)
        return
        
    run_segment_names = sorted([x for x in all_files if '.evt' in x])

    # now start the conversion of each segment and append to the h5 file
        
    if not h5_filename:
        h5_filename = h5_path + run_string + '.h5'

    # compression  for the tables file
    h5_filters = tb.Filters(complevel=complevel, complib='blosc')

    f = tb.open_file(h5_filename, mode='w', filters=h5_filters)

    raw_data = f.create_group(f.root, 'raw_data', 'Unsorted Data', filters=h5_filters)
    table = f.create_table(raw_data, 'basic_info',  DetectorHit, 'PXI 16 event header Info', expectedrows=1000000)
    trace_array = f.create_vlarray(f.root.raw_data, 'trace_array', tb.Int32Atom(shape=()), 'Trace Data', filters=h5_filters)
    
    for i, ele in enumerate(run_segment_names):
        print('Working on ' + ele)
        full_path = exp_path + run_string + '/' + ele
        nscl_convert.read_evt(full_path, table, trace_array, event_chunk_size=event_chunk_size)


    print('Finished Conversion')

    f.flush()
    f.close()
    


def convert_faster_run(faster_filename,
                h5_filename=None, h5_path='',
                event_chunk_size=100000, complevel=3):


    # name the h5 file

    run_name = faster_filename.split('.')[0]

    h5_filename = h5_path + run_name  + '.h5'

    # compression  for the tables file
    h5_filters = tb.Filters(complevel=complevel, complib='blosc')

    f = tb.open_file(h5_filename, mode='w', filters=h5_filters)

    raw_data = f.create_group(f.root, 'raw_data', 'Unsorted Data', filters=h5_filters)
    table = f.create_table(raw_data, 'basic_info',  DetectorHit, 'FASTER Event Info', expectedrows=1000000)
    trace_array = f.create_vlarray(f.root.raw_data, 'trace_array', tb.Int32Atom(shape=()), 'Trace Data', filters=h5_filters)
    
    print('Working on ' +  run_name)
    faster_convert.read_evt(faster_filename, table, trace_array, event_chunk_size=event_chunk_size)


    print('Finished Conversion')

    f.flush()
    f.close()

