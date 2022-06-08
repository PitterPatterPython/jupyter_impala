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
import jupyter_integrations_utility as jiu
# Put any additional imports specific to your integration here: 
import pyodbc as po

@magics_class
class Impala(Pyodbc):
    # Static Variables
    # The name of the integration
    # The class name (Start) should be changed to match the name_str, but with the first letter upper cased.
    name_str = "impala"
    instances = {}
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

    def __init__(self, shell, debug=False, *args, **kwargs):
        super(Impala, self).__init__(shell, debug=debug)
        self.debug = debug

        #Add local variables to opts dict
        for k in self.myopts.keys():
            self.opts[k] = self.myopts[k]

        self.load_env(self.custom_evars)
        self.parse_instances()


    # Overriding Custom Query to handle thrift errors and auto matic resubmit
    def customQuery(self, query, instance):
        mydf = None
        status = ""
        resubmit = False
        try:
            self.instances[instance]['session'].execute(query)
            mydf = self.as_pandas_DataFrame(instance)
            if mydf is not None:
                status = "Success"
            else:
                status = "Success - No Results"
        except Exception as e:
            mydf = None
            str_err = str(e)
            if self.debug:
                print("Error: %s" % str_err)
            if str_err.find("Impala Thrift API") >= 0 and str_err.find("SSL_write: bad write retry") >= 0:
                if resubmit == False:
                    # This is an old connection, let's just resubmit it (once)
                    print("SSL_write Thrift error detected - Likely Stale Connection - Attempting 1 retry")
                    try:
                        resubmit = True # First we make sure we only resubmit once
                        self.instances[instance]['session'].execute(query)
                        mydf = self.as_pandas_DataFrame(instance)
                        if mydf is not None:
                            status = "Success"
                        else:
                            status = "Success - No Results"
                    except Exception as e1:
                        mydf = None
                        str_err1 = str(e1)
                        final_err = "First Run: %s\nSecond Run: %s"  % (str_err, str_err1)
                        if self.debug:
                            print("Second Run Error: %s" % str_err1)
                        status = "Failure - query_error: " % final_err
            else:
                status = "Failure - query_error: " + str_err
        return mydf, status


# def customDisconnect - In pyodbc
# def customAuth - In pyodbc
# def validateQuery - In pyodbc
# def customQuery - In pyodbc
# def customHelp - In pyodbc
    def retCustomDesc(self):
        return "Jupyter integration for working with Cloudera Impala via PyODBC based data sources"

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




