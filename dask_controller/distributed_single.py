#!/usr/bin/env python

import os, sys
import subprocess
from dask.distributed import Client
from time import sleep


class distributed_single:
    '''import to run distributed single against the existing task scheduler'''
    def __init__(self, **kwargs):
        '''find scheduler and return object'''
        self.address = 'tcp://10.0.1.2:8786' #default address
        if kwargs.get('client'):
            self.address = str(kwargs['client'])
        #if kwargs.get('logger'):  # implement this later
        #    self.logger = logger
        
        self.client = Client(self.address, processes=False) #connect
 
