#!/usr/bin/python

# Base imports for all integrations, only remove these at your own risk!
import json
import sys
import os
import time
import pandas as pd
from collections import OrderedDict
import requests
from integration_core import Integration
from pyodbc_core import Pyodbc
    
from IPython.core.magic import (Magics, magics_class, line_magic, cell_magic, line_cell_magic)
from IPython.core.display import HTML

#import IPython.display
from IPython.display import display_html, display, Javascript, FileLink, FileLinks, Image
import ipywidgets as widgets

# Put any additional imports specific to your integration here: 
import pyodbc as po

@magics_class
class Impala(Pyodbc):
    # Static Variables
    # The name of the integration
    # The class name (Start) should be changed to match the name_str, but with the first letter upper cased.
    name_str = "impala"

    # These are the ENV variables the integration will check when starting up. The integration_base prefix will be prepended in checking (that defaults to JUPYTER_) 
    # So the following two items will look for:
    # JUPYTER_START_BASE_URL and put it into the opts dict as start_base_url
    # JUPYTER_START_USER as put it in the opts dict as start_user
    custom_evars = ["impala_conn_default"]


    # These are the variables in the opts dict that allowed to be set by the user. These are specific to this custom integration and are joined
    # with the base_allowed_set_opts from the integration base
    # The three examples here would be "start_base_url, start_ignore_ssl_warn, and start_verbose_errors
    # Make sure these are defined in myopts!
    custom_allowed_set_opts = ["impala_conn_default"]



    # These are the custom options for your integration    
    myopts = {}

    # These are the custom options for your integration    
    myopts = {}
    myopts['impala_max_rows'] = [1000, 'Max number of rows to return, will potentially add this to queries']
    myopts['impala_conn_default'] = ["default", 'Default instance name for connections']

    # Class Init function - Obtain a reference to the get_ipython()
    def __init__(self, shell, pd_display_grid="html", impala_conn_url_default="", debug=False, *args, **kwargs):
        super(Impala, self).__init__(shell, debug=debug, pd_display_grid=pd_display_grid) # Change the class name (Start) to match your actual class name
        self.debug = debug

        self.opts['pd_display_grid'][0] = pd_display_grid
        if pd_display_grid == "qgrid":
            try:
                import qgrid
            except:
                print ("WARNING - QGRID SUPPORT FAILED - defaulting to html")
                self.opts['pd_display_grid'][0] = "html"

        #Add local variables to opts dict
        for k in self.myopts.keys():
            self.opts[k] = self.myopts[k]

        self.load_env(self.custom_evars)
        if impala_conn_url_default != "":
            if "default" in self.instances.keys():
                print("Warning: default instance in ENV and passed to class creation - overwriting ENV")
            self.fill_instance("default", impala_conn_url_default)

        self.parse_instances()

# def customDisconnect - In pyodbc
# def customAuth - In pyodbc
# def validateQuery - In pyodbc
# def customQuery - In pyodbc
# def customHelp - In pyodbc


    # This is the magic name.
    @line_cell_magic
    def impala(self, line, cell=None):
        if cell is None:
            line = line.replace("\r", "")
            line_handled = self.handleLine(line)
            if self.debug:
                print("line: %s" % line)
                print("cell: %s" % cell)
            if not line_handled: # We based on this we can do custom things for integrations. 
                if line.lower() == "testintwin":
                    print("You've found the custom testint winning line magic!")
                else:
                    print("I am sorry, I don't know what you want to do with your line magic, try just %" + self.name_str + " for help options")
        else: # This is run is the cell is not none, thus it's a cell to process  - For us, that means a query
            self.handleCell(cell, line)




