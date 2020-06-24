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
    custom_evars = [name_str + "_dsn", name_str + "_host", name_str + "_port", name_str + "_user", name_str + "_default_db"]


    # These are the variables in the opts dict that allowed to be set by the user. These are specific to this custom integration and are joined
    # with the base_allowed_set_opts from the integration base
    # The three examples here would be "start_base_url, start_ignore_ssl_warn, and start_verbose_errors
    # Make sure these are defined in myopts!
    custom_allowed_set_opts = [name_str + '_authmech', name_str + "_usesasl", name_str + "_usessl", name_str + "_allowselfsignedcert", name_str + "_dsn", name_str + "_host", name_str + "_port", name_str + "_user", name_str + "_default_db"] 



    # These are the custom options for your integration    
    myopts = {} 
    myopts[name_str + '_max_rows'] = [1000, 'Max number of rows to return, will potentially add this to queries']
    myopts[name_str + '_user'] = ["impala", "User to connect with  - Can be set via ENV Var: JUPYTER_" + name_str.upper() + "_USER otherwise will prompt"]
    myopts[name_str + '_dsn'] = ["MyDSN", "DSN name registered with ODBCt via ENV Var: JUPYTER_" + name_str.upper() + "_DSN"]
    myopts[name_str + '_host'] = ["myhost.mydomain.local", "Host name to connect via ENV Var: JUPYTER_" + name_str.upper() + "_HOST"]
    myopts[name_str + '_port'] = ["21050", "Port to use to connect via ENV Var: JUPYTER_" + name_str.upper() + "_PORT"]
    myopts[name_str + '_default_db'] = ["default", "Default DB (defaults to default) via ENV Var: JUPYTER_" + name_str.upper() + "_DEFAULT_DB"]

    myopts[name_str + '_authmech'] = ["3", "Passed to the ODBC AuthMech setting (Defaults to 3)"]
    myopts[name_str + '_usesasl'] = ["1", "Passed to the ODBC UseSASL (Defaults to 1)"]
    myopts[name_str + '_usessl'] = ["1", "Passed to the ODBC SSL (Defaults to 1)"]
    myopts[name_str + '_allowselfsignedcert'] = ["0", "Passed to the ODBC AllowSelfSignedServerCert (Defaults to 0)"]

    myopts[name_str + '_last_query'] = ["", "The last query attempted to be run"]
    myopts[name_str + '_last_use'] = ["", "The use (database) statement ran"]

    # Class Init function - Obtain a reference to the get_ipython()
    def __init__(self, shell, pd_display_grid="html", authmech="3", usesasl="1", usessl="1", allowselfsignedcert="0", *args, **kwargs):
        super(Impala, self).__init__(shell, pd_display_grid, authmech, usesasl, usessl, allowselfsignedcert) # Change the class name (Start) to match your actual class name

    # No need to change this code
        self.load_env(self.custom_evars) 
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
        # Sets items from Class init. Modify if you modify the class init
        self.opts['pd_display_grid'][0] = pd_display_grid
        self.opts[self.name_str + "_authmech"][0] = authmech
        self.opts[self.name_str + "_usesasl"][0] = usesasl
        self.opts[self.name_str + "_usessl"][0] = usessl
        self.opts[self.name_str + "_allowselfsignedcert"][0] = allowselfsignedcert

    
    def disconnect(self):
        if self.connected == True:
            print("Disconnected %s Session from %s" % (self.name_str.capitalize(), self.opts[self.name_str + '_host'][0]))
        else:
            print("%s Not Currently Connected - Resetting All Variables" % self.name_str.capitalize())
        self.session = None
        try:
            self.connection.close()
        except:
            pass
        self.connection = None
        self.connect_pass = None
        self.connected = False


    def connect(self, prompt=False):
        if self.connected == False:
            if prompt == True or self.opts[self.name_str + '_user'][0] == '':
                print("User not specified in %s%s_USER or user override requested" % (self.env_pre, self.name_str.upper()))
                tuser = input("Please type user name if desired: ")
                self.opts[self.name_str + '_user'][0] = tuser
            print("Connecting as user %s" % self.opts[self.name_str + '_user'][0])
            print("")
            if prompt == True or self.opts[self.name_str  + "_dsn"][0] == '':
                print("%s dsn not specified in %s%s_DSN or override requested" % (self.env_pre, self.name_str.capitalize(), self.name_str.upper()))
                tdsn = input("Please type in the full %s DSN: " % self.name_str.capitalize())
                self.opts[self.name_str + '_dsn'][0] = tdsn
            print("Connecting to %s DSN: %s" % (self.name_str.capitalize(), self.opts[self.name_str + '_dsn'][0]))
            print("")

            if prompt == True or self.opts[self.name_str  + "_host"][0] == '':
                print("%s Host not specified in %s%s_HOST or override requested" % (self.env_pre, self.name_str.capitalize(), self.name_str.upper()))
                thost = input("Please type in the full %s HOST: " % self.name_str.capitalize())
                self.opts[self.name_str + '_host'][0] = thost
            print("Connecting to %s HOST: %s" % (self.name_str.capitalize(), self.opts[self.name_str + '_host'][0]))
            print("")

            if prompt == True or self.opts[self.name_str  + "_port"][0] == '':
                print("%s Port not specified in %s%s_PORT or override requested" % (self.env_pre, self.name_str.capitalize(), self.name_str.upper()))
                tport = input("Please type in the full %s PORT: " % self.name_str.capitalize())
                self.opts[self.name_str + '_port'][0] = tport
            print("Connecting to %s PORT: %s" % (self.name_str.capitalize(), self.opts[self.name_str + '_port'][0]))
            print("")

            if prompt == True or self.opts[self.name_str  + "_default_db"][0] == '':
                print("%s Default DB not specified in %s%s_DEFAULT_DB or override requested" % (self.env_pre, self.name_str.capitalize(), self.name_str.upper()))
                tdefaultdb = input("Please type in the %s DEFAULT_DB: " % self.name_str.capitalize())
                self.opts[self.name_str + '_default_db'][0] = tdefaultdb
            print("Connecting to %s DEFAULT_DB: %s" % (self.name_str.capitalize(), self.opts[self.name_str + '_default_db'][0]))
            print("")

#            Use the following if your data source requries a password # Or comment out 
            print("Please enter the password you wish to connect with:")
            tpass = ""
            self.ipy.ex("from getpass import getpass\ntpass = getpass(prompt='Connection Password: ')")
            tpass = self.ipy.user_ns['tpass']

            self.connect_pass = tpass
            self.ipy.user_ns['tpass'] = ""

            # once information is gathered, run the auth routine

            result = self.auth()

            if result == 0:
                self.connected = True
                print("%s - %s Connected!" % (self.name_str.capitalize(), self.opts[self.name_str + '_host'][0]))
            else:
                print("Connection Error - Perhaps Bad Usename/Password?")

        elif self.connected == True:
            print(self.name_str.capitalize() + "is already connected - Please type %" + self.name_str + " for help on what you can you do")

        if self.connected != True:
            self.disconnect()

    def auth(self):
        self.session = None
        result = -1
        n = self.name_str
        kar = [n + "_dsn", n + "_host", n + "_port", n + "_default_db", n + "_authmech", n + "_usesasl", n + "_user", n + "_usessl", n + "_allowselfsignedcert"]
        var = []
        for x in kar:
            var.append(self.myopts[x][0])
            if x == n + "_user":  # Sneak in the password
                var.append(self.connect_pass)
        
        # Create a session variable if needed
        conn_string = "DSN=%s; Host=%s; Port=%s; Database=%s; AuthMech=%s; UseSASL=%s; UID=%s; PWD=%s; SSL=%s; AllowSelfSignedServerCert=%s" % (var[0], var[1], var[2], var[3], var[4], var[5], var[6], var[7], var[8], var[9])

        try:
            self.connection = po.connect(conn_string, autocommit=True)
            self.session = self.connection.cursor()
            result = 0
        except Exception as e:
            str_err = str(e)
            print("Unable to connect Error:\n%s" % str_err)
            result = -2

        # Here you can check if the authentication on connect is successful. If it's good, return 0, otherwise return something else and show an error

        return result


    def validateQuery(self, query):
        bRun = True
        bReRun = False
        if self.opts[self.name_str + "_last_query"][0] == query:
            # If the validation allows rerun, that we are here:
            bReRun = True
        # Ok, we know if we are rerun or not, so let's now set the last_query 
        self.opts[self.name_str + "_last_query"][0] = query


        # Example Validation

        # Warn only - Don't change bRun
        # This one is looking for a ; in the query. We let it run, but we warn the user
        # Basically, we print a warning but don't change the bRun variable and the bReRun doesn't matter
        if query.find(";") >= 0:
            print("WARNING - Do not type a trailing semi colon on queries, your query will fail (like it probably did here)")

        # Warn and don't submit after first attempt - Second attempt go ahead and run
        # If the query doesn't have a day query, then maybe we want to WARN the user and not run the query.
        # However, if this is the second time in a row that the user has submitted the query, then they must want to run without day
        # So if bReRun is True, we allow bRun to stay true. This ensures the user to submit after warnings
        if query.lower().find("limit ") < 0:
            print("WARNING - Queries shoud have a limit so you don't bonkers your DOM")
        # Warn and do not allow submission
        # There is no way for a user to submit this query 
#        if query.lower().find('limit ") < 0:
#            print("ERROR - All queries must have a limit clause - Query will not submit without out")
#            bRun = False
        return bRun

    def customQuery(self, query):
        mydf = None
        status = ""
        try:
            self.session.execute(query)
            mydf = self.as_pandas_DataFrame()
            if mydf is not None:
                status = "Success"
            else:
                status = "Success - No Results"
        except Exception as e:
            mydf = None
            str_err = str(e)
            if self.debug:
                print("Error: %s" % str(e))
                status = "Failure - query_error: " + str_err
        return mydf, status




# Display Help must be completely customized, please look at this Hive example
    def customHelp(self):
        print("Help for Impala")


    # This is the magic name.
    @line_cell_magic 
    def impala(self, line, cell=None):  # Change the name of this function to your class name all lower cased
        if cell is None:
            line = line.replace("\r", "")
            line_handled = self.handleLine(line)
            if self.debug:
                print("line: %s" % line)
                print("cell: %s" % cell)
            if not line_handled: # We based on this we can do custom things for integrations. 
                print("I am sorry, I don't know what you want to do with your line magic, try just %" + self.name_str + "for help options")
        else: # This is run is the cell is not none, thus it's a cell to process  - For us, that means a query
            cell = cell.replace("\r", "")
            self.handleCell(cell)

