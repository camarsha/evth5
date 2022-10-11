"""
Unpack the data in evt files.

Caleb Marshall, Ohio University April 2022
"""

from struct import unpack
import struct
import numpy as np
from tqdm import tqdm
import tables as tb

masks = {'3bit': 0x7,
         '4bit': 0xf,
         '5bit': 0x1f,
         '13bit': 0x1fff, 
         '14bit':0x3fff,
         '15bit': 0x7fff,
         '16bit':0xffff,
         '1bit': 0x1}

def read4(filebuffer, size):
    value = unpack('<I', filebuffer.read(4))[0]
    size -= 4
    return value, size

    
class DDASHit():

    # constants
    qdc_len = 8
    ext_ts_len = 2
    raw_energy = 4

    
    def __init__(self, adc_freq):
        
        self.freq = adc_freq

        # CFD conversion changes based on the frequency.    
        if adc_freq == 100:
            self.cfd = self.cfd_100
        elif adc_freq == 250:
            self.cfd = self.cfd_250
        elif adc_freq == 500:
            self.cfd = self.cfd_500
        else:
            print("Module frequency not valid!!")
            print("Invalid Frequency:", adc_freq)
            
        self.trace = []
        self.qdc = np.zeros(8, dtype=np.int32)

    
    def read_hit(self, filebuffer, size):
      
        # first 4 bytes
        header, size = read4(filebuffer, size)
        
        # I barely know what I am doing, but this works
        # DDAS masks then shifts which leads to the abundance of masks
        # This shifts then masks, which I think makes me less prone to error
        self.channel = header & masks['4bit']
        self.slot = (header >> 4) & masks['4bit']
        self.crate = (header >> 8) & masks['4bit']
        self.header_size = (header >> 12) & masks['5bit']
        # this is the total size of the event in *bit words
        self.event_length = (header >> 17) & masks['14bit']
        self.finish_code = (header >> 31) & masks['1bit']

        
        # timestamp word
        self.time_low, size = read4(filebuffer, size)

        # CFD and timestamp
        cfd_word, size = read4(filebuffer, size)
        self.cfd(cfd_word)

        # energy and trace length
        energy_word, size = read4(filebuffer, size)
        self.energy = energy_word & masks['16bit']
        self.trace_length = (energy_word >> 16) & masks['15bit']
        self.overflow = (energy_word >> 31) & masks['1bit']
        

        # optional blocks

        opt_len = self.event_length - (int(self.trace_length/2) + 4)
        # only support qdc right now
        if opt_len == 8:
            size = self.set_qdc(filebuffer, size)
        
        # lets take care of the trace
        if self.trace_length:
            size = self.set_trace(filebuffer, size)
                
        return size 

    def set_trace(self, filebuffer, size):

        l = [unpack('<H', filebuffer.read(2))[0] for x in range(self.trace_length)]
        self.trace = np.asarray(l, dtype='int32')
        size -= int(2*self.trace_length)

        return size

    def set_qdc(self, filebuffer, size):
        for i in range(8):
            self.qdc[i], size = read4(filebuffer, size)
        return size
            
    def cfd_100(self, cfd_info):
        """
        100 MHz module, see PXI manual for details
        """
        self.time_high = cfd_word & masks['16bit']
        self.cfd_frac = (cfd_word >> 16) & masks['15bit']
        self.cfd_force = (cfd_word >> 31) & masks['1bit']

        # calculate the raw and corrected time in ns
        evt  = self.time_low + (self.time_high*2**32)
        self.time_raw = evt * 10.0
        self.time = (evt + self.cfd_frac/32768.0) * 10.0

    def cfd_250(self, cfd_word):
        """
        250 MHz module, see PXI manual for details
        """
        self.time_high = cfd_word & masks['16bit']
        self.cfd_frac = (cfd_word >> 16) & masks['14bit']
        self.parity = (cfd_word >> 30) & masks['1bit']
        self.cfd_force = (cfd_word >> 31) & masks['1bit']

        # calculate the raw and corrected time in ns
        evt  = self.time_low + (self.time_high*2**32)
        self.time_raw = evt * 8.0
        self.time = (evt * 2 - self.parity + self.cfd_frac/16384.0) * 4.0
        
    def cfd_500(self, cfd_info):
        """
        500 MHz module, see PXI manual for details
        """
        self.time_high = cfd_word & masks['16bit']
        self.cfd_frac = (cfd_word >> 16) & masks['13bit']
        self.cfd_trigger_source = (cfd_word >> 31) & masks['3bit']

        # calculate the raw and corrected time in ns
        evt  = self.time_low + (self.time_high*2**32)
        self.time_raw = evt * 10.0
        self.time = (evt * 5 + self.cfd_trigger_source - 1 + self.cfd_frac/8192.0) * 2.0
        

    def get_data(self):
        return self.crate, self.slot, self.channel, self.energy, self.time_raw, self.time, self.qdc
    

def hits_to_rows(row, array, hits):
    """
    Loop to append detector info to pytables row object
    """
    
    for ele in hits:
        (row['crate'], row['slot'], row['channel'],
         row['energy'], row['time_raw'], row['time'],
         qdc_temp) = ele.get_data()

        (row['qdc_0'], row['qdc_1'], row['qdc_2'], row['qdc_3'],
         row['qdc_4'], row['qdc_5'], row['qdc_6'], row['qdc_7']) = qdc_temp
        
        if ele.trace != []:
            array.append(ele.trace)
            row['is_trace'] = True
            row['trace_idx'] = array.nrows
        else:
            row['is_trace'] = False
            
        row.append()
    

def read_header(filebuffer):
    header_size, header_type = unpack('<II', filebuffer.read(8))
    header_size -= 8 # exclusive size
    return header_size, header_type

def body_header(filebuffer, size):
    # body header is useless crap that is 20 bytes long 
    filebuffer.read(20)
    size -= 20
    return size

def fragment_header(filebuffer, size):
    # skip size of fragments
    filebuffer.read(4)
    size -= 4
    return size

def body(filebuffer, size):
    # skip fragment header
    filebuffer.read(20)
    size -= 20
    # physics header skip
    filebuffer.read(8)
    size -= 8
    #another body header skip
    filebuffer.read(20)
    size -= 20
    body_size, adc_freq, adc_res, revision = unpack('<ihbb', filebuffer.read(8))
    size -= 8

    return adc_freq, size 
    
def physics_event(filebuffer, size):

    # list of ddas hits
    hits = []
    trace_count = 0
    size = body_header(filebuffer, size)
    # now loop over the fragments
    size = fragment_header(filebuffer, size)
    while size != 0:
        adc_freq, size = body(filebuffer, size)
        hit = DDASHit(adc_freq)
        size = hit.read_hit(filebuffer, size)
        hits.append(hit)
        if hit.trace != []:
            trace_count += 1
        
        
    return hits

        

def read_evt(evt_filename, table, array, event_chunk_size=100000):
    """
    Idea is to read file until a set number of events have been processed.
    Then transfer to the 

    Note "events" is used loosely here. An event is a built event, which 
    means there might be multiple channel hits per "event". 

    """

    bytes_left = True


    # generator so tqdm can be used on the while condition
    def read_loop():
        while bytes_left:
            yield

    
    with open(evt_filename, 'rb') as f:

        # pytable setup
        row = table.row
        
        evt_count = 0
        print('Physics Events Processed: ')
        all_hits = []
        for _ in tqdm(read_loop()):
            
            if evt_count >= event_chunk_size:
                # periodically clear memory and save to disk
                hits_to_rows(row, array, all_hits)
                table.flush()
                array.flush()
                evt_count = 0
                all_hits = []

            # loop ends when we fail to find enough bytes to read
            try:
                hs, ht = read_header(f)
                # physics events
                if ht == 30:
                    hits = physics_event(f, hs)
                    all_hits += hits
                    evt_count += 1
                    # skip the other junk
                else:
                    f.read(hs)
            except struct.error:
                bytes_left = False
                

        # get the last set of events if they exist
        if all_hits:
            hits_to_rows(row, array, all_hits)
            table.flush()
            array.flush()
        
        
