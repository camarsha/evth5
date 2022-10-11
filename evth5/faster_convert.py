"""
Run conversion for faster digitizers.

-Caleb Marshall, Ohio University 2022   
"""

import faster
import numpy as np
from tqdm import tqdm
from .nscl_convert import DDASHit
from .nscl_convert import hits_to_rows


class FasterHit(DDASHit):
    """
    Lot of fluff in this class because the pytables format I set
    up is really inflexible.
    """
    
    def __init__(self, channel, energy, time):
        self.crate =  0
        self.slot = 0
        self.channel = channel
        self.energy = energy
        self.time_raw = time
        self.time = time * 2.0
        self.qdc = np.zeros(8, dtype=np.int32)
        self.trace = []
        


def faster_hit(evt):
    return FasterHit(evt.label, evt.data['value'], evt.time)
    
def faster_multi_hit(evt):
    hits = []
    for hit in evt.data['events']:
        hits.append(FasterHit(hit.label, hit.data['value'], hit.time))
    return hits
        

def read_evt(faster_filename, table, array, event_chunk_size=100000, build_label=3000):
    """
    Faster file reader does all the unpacking for us, so this is just a
    simple interface to pytables.

    """
    
    with faster.FileReader(faster_filename) as f:

        # pytable setup
        row = table.row
        evt_count = 0
        print('Physics Events Processed: ')
        all_hits = []
        for evt in tqdm(f):
            
            if evt_count >= event_chunk_size:
                # periodically clear memory and save to disk
                hits_to_rows(row, array, all_hits)
                table.flush()
                array.flush()
                evt_count = 0
                all_hits = []

            # ignoring built events for now
            if evt.label == 1 or evt.label == 2:
                hit = faster_hit(evt)
                all_hits.append(hit)
                evt_count += 1
            elif evt.label == build_label:
                temp = faster_multi_hit(evt)
                evt_count += len(temp)
                all_hits += temp

        # get the last set of events if they exist
        if all_hits:
            hits_to_rows(row, array, all_hits)
            table.flush()
            array.flush()
        
        

