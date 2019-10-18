# -*- coding: utf-8 -*-
"""
Three panda-related assignment ideas. All using one percent data

1) Compare Alabama and CA data. (intro to query language)
[too vague as yet]

2)  Construct a graph examining the correlation
of two person variables (education and income).
Ask them to write a script making the necessary queries
to collect data points over intervals on these
two interval variables.

3) Create an extract in some specfic format,
catering to particular needs.
Use the HOUSEHOLD concept (both household and person records have a
field for the household serial number [field SERIALNO], establishing
a link that can be used to aggregate data about asingle household.)
(R format: data frame)
[The dataframe class below is an example-solution to this.  CReates
a Python dataframe-like object from some specified
subset of the census DB and saves it to a file
in a format readable by R.]

CREATE A DATAMODEL
==================

Basically all the data is stored in bunch of long strings
with the various bits of information weirdly encoded.  A data model tells
what kind of information is available, where each kind of information
is stored, and what the weird encodings mean.

The PUMS site defines most of the important
parts of thedata model in one big Excel sheet.
The CensusInfo class below reads in the Excel sheet,
stores a queryable Python version of the data madel, and
provides read functions for reading all or part of the
actual PUMS data (stored in big text files on the website)
in to a usable Python data structure.

        >>> ci = CensusInfo (xl_file)

Converts the EXcel sheet
into an internal Python data dictionary. Provides some
simple data retrieval capability through C{select_rows} and some
synonyms for important columns. Provides explanations of the
variables and variable values through the C{var_info} method.


Also provides some simple sata retrieval capability from
the actual PUMS data.  But the data retrieval capability is minimal.
Leaves the row strings that are actually
complex DB records as strings an d does not convert them
into any native Python structures.

    Examples
    =======

    >>> ci.print_var_info('RELATE',ci.person_record,ci.one_percent_file)
    Person Record RELATE 1% file {u'RT': u'P', u'BEG': 17, u'LEN': 2, u'DESCRIPTION': u'Relationship'}

     Relationship
     ============
     01  Householder
     02  Husband/wife
     03  Natural born son/daughter
     04  Adopted son/daughter
     05  Stepson/Stepdaughter
     06  Brother/sister
     07  Father/mother
     08  Grandchild
     09  Parent-in-law
     10  Son-in-law/daughter-in-law
     11  Other relative
     12  Brother-in-law/sister-in-law
     13  Nephew/niece
     14  Grandparent
     15  Uncle/aunt
     16  Cousin
     17  Roomer/boarder
     18  Housemate/roommate
     19  Unmarried partner
     20  Foster child
     21  Other nonrelative
     22  Institutionalized GQ person
     23  Noninstitutionalized GQ person

    >>> rows2 = ci.select_rows([('race','47'),('education',13,operator.gt) ], \
        ci.person_record, five_percent_data_file, ci.five_percent_file, verbose=True)
        
    race       eq  47  3.4684% of the Person Records
    education  gt  13  

    rows2 is a list of row strings from the DB.

    The intended usage is to use the dataframe class to
    make a Python internal rep of actual parts of a
    PUMS db.  The best way to do that is using the Panda interface.


CREATE A DATAFRAME (R)
=====================
(idea from Norman Matloff's Art of R Programming)

A dataframe is a matrix with labeled cols (dont
know if rows can also be labeled; dont have to be)

More specifically R vectors (1 dimension) and matrices (2 dimensions)
have to be homogeneous (all  cells filled with entities of the same type).
Each has
a heterogeneous analogue: vector => list; matrix=> data frame.

Python has nothing analogous.  The closest in terms of the raw data
requirement would be a list of lists, but there is no way to cleanly
slice out a column in a list of lists, or to carve out arbitatry
subtables, so much of the flexibility of a dataframe is absent.  Numpy
arrays have all this, but must store homogeneous data types (all float
32 or float 64 or all strings), and that is quite a limitation.

Soln 1:  Use pandas (http://pandas.pydata.org/), which really has done
a very good implementation of a dataframe.  By far the best soln
Pandas is a big install, but it is  included in the Enthought dist of Python.

Soln2  Use
Keith Goodman's labeled array package (https://pypi.python.org/pypi/la)
[simpler, also built on top of numpy arrays]. Labeled arrays
are called "Larries".  They
are implemented as instance objects with a pair of lists (labels for cols, rows)
and a numpy array. Larries are therefore not size
mutable and cannot store heterogenous data, like
real R dataframes.

Soln3:  Native Python solution 1. An instance object with a pair of lists (labels for cols, rows)
and a numpy array.  Whcih is essentially whata larry is,
and therefore suffers from the same problem.

Soln4:  Native Python soln 2. A Python list of lists. with associated labelling.
So, an instance object wrapped around a list.  No 

"""
#from xlIO import xlReader

import xlrd,re,operator
from collections import defaultdict, Counter
import pandas as pd

def format_list(xl_sheet,keys=['URL'], encoding=None):
    """
    Get the values in the first line of the excel sheet which is
    such that all keys values are found.  A key is a col_name that must
    be found.   We allow for ntuple keys.

    This line is  assumed to  label the columns in the excel sheet (format line)
    """
    searching = True
    is_empty_str = lambda x: x == ''
    ctr = 0
    while searching:
        try:
            rvs = xl_sheet.row_values(ctr)
            searching = any((x not in rvs for x in keys))
            #searching = not (key in rvs)
        except IndexError:
            print 'No format row found through row %d' % (ctr,)
            return []
        ctr += 1
    if encoding:
        return [encoding(v)[0] for v in rvs]
    else:
        return rvs

def decompose_spec (spec):
    """
    For query specs used in very rudimerntary queriy language.

    Conceptually a field/val/op triple, but
    op is optional (interpreted as the default
    op eq when absent).
    """
    try:
        (field,val,op) = spec
    except ValueError:
        (field, val) = spec
        op = operator.eq
    return (field,val,op)

##############################################################################
##############################################################################
##
##     D a t a       C l a s s
##
##############################################################################
##############################################################################

class CensusInfo (object):

    """
    Reads in an Excel sheet data dictionary from the Census data website
    for PUMS data (Public Use MicroData).

        >>> ci = CensusInfo (data_model_file)

    Converts into an internal Python data dictionary. Provides some
    simple data retrieval capability through C{select_rows} and some
    synonyms for important columns. Provides explanations of the
    variables and variable values through the C{var_info} method.

    But does very litle.  Leaves the row strings that are actually
    complex DB records as strings an d does not convert them
    into any native Python structures.
                                                          
    Examples
    =======

    >>> ci.print_var_info('RELATE',ci.person_record,ci.one_percent_file)
    Person Record RELATE 1% file {u'RT': u'P', u'BEG': 17, u'LEN': 2, u'DESCRIPTION': u'Relationship'}

     Relationship
     ============
     01  Householder
     02  Husband/wife
     03  Natural born son/daughter
     04  Adopted son/daughter
     05  Stepson/Stepdaughter
     06  Brother/sister
     07  Father/mother
     08  Grandchild
     09  Parent-in-law
     10  Son-in-law/daughter-in-law
     11  Other relative
     12  Brother-in-law/sister-in-law
     13  Nephew/niece
     14  Grandparent
     15  Uncle/aunt
     16  Cousin
     17  Roomer/boarder
     18  Housemate/roommate
     19  Unmarried partner
     20  Foster child
     21  Other nonrelative
     22  Institutionalized GQ person
     23  Noninstitutionalized GQ person

    >>> rows2 = ci.select_rows([('race','47'),('education',13,operator.gt) ], \
        ci.person_record, five_percent_data_file, ci.five_percent_file, verbose=True)
        
    race       eq  47  3.4684% of the Person Records
    education  gt  13  

    rows2 is a list of row strings from the DB.

    Build one of these.  Then use the dataframe class to
    make a Python internal rep of actual parts of a
    PUMS db.

     >>> ci = CensusInfo (xl_file)
     >>> df = DataFrame(ci)
    >>> df.fill_frame ([], ci.person_record, one_percent_data_file, ci.one_percent_file, \
                   'race','education','income','gender','age','relationship')
    
    >>> one_percent_extract = os.path.join(one_percent_dir,'revisedpums1_alabama_01_race_education_income_gender_age_relationship_with_headers.dat')
    >>> df.save_frame(one_percent_extract)
    """

    file_types = [u'5% file', u'1% file']
    five_percent_file = file_types[0]
    one_percent_file = file_types[1]
    record_types = [u'Housing Unit Record', u'Person Record']
    household_record = record_types[0]
    person_record = record_types[1]
    record_type_codes = [u'H', u'P']
    record_type_dict = dict(zip(record_types,record_type_codes))
    # where the variables names are in the excel sheet
    # DB dictionary
    (variable_field,variable_col) = (u'VARIABLE',5)
    key = 'serial_no'

    # Convenient field names for user.  List of important fields
    synonyms = dict(education = 'EDUC',
                    race = 'RACE3',
                    nonnative_speaker = 'ENGABIL',
                    language5 = 'LANG5', #5% file
                    language1 = 'LANG1', #1% file
                    citizen = 'CITIZEN',
                    married = 'MARSTAT',
                    income = 'INCWS',
                    num_persons = 'PERSONS', # Num persons in household (H recs only)
                    serial_no = 'SERIALNO', # Household serial number (links H&P records)
                    white = 'WHITE',
                    black = 'BLACK',
                    asian = 'ASIAN',
                    gender = 'SEX',
                    age = 'AGE ',
                    relationship = 'RELATE'
                    )

    #  Functional properties of the variable, e.g., its start end location in the record,
    #  its explanation, one of each oer variable
    variable_properties_list = [u'RT', u'BEG',u'LEN',u'DESCRIPTION']
    variable_properties_cols = [0,1,3,6]
    variable_properties_dict   = \
          dict(zip(variable_properties_list,variable_properties_cols))

    #  List or description of all the values the variable can have.
    variable_explanation_list = [u'LO',u'HI',u'VALUE DESCRIPTION']
    variable_explanation_cols_dict = {five_percent_file: [7,8,9],
                                 one_percent_file: [10,11,12],
                                 }

    col_names = variable_properties_list + variable_explanation_list + [variable_field]
    
    for file_type in variable_explanation_cols_dict.keys():
        variable_explanation_cols_dict[file_type] = \
                         dict(zip(variable_explanation_list,variable_explanation_cols_dict[file_type]))

    def __init__ (self, data_model_file):
        """
        C{cata_model_file} is a census format EXcel sheet containing
        the data model.
        """

        ## A default val
        self.file_type = self.file_types[0]
        xl_book = xlrd.open_workbook(data_model_file,formatting_info=True)
        #  Functional properties of the variable, e.g., its start end location in the record,
        #  its explanation, one of each oer variable
        data_dictionary = dict()
        self.data_dictionary = data_dictionary
        #  List or description of all the values the variable can have.
        variable_values_dictionary = dict()
        self.variable_values_dictionary = variable_values_dictionary

        for sheet_index in range(xl_book.nsheets):
            thesheet =  xl_book.sheet_by_index(sheet_index)
            # all_col_labels not used.  We use this as a wellformedness test
            all_col_labels = format_list(thesheet,keys=self.col_names)
            record_type = thesheet.name

            ## Two different data universes in the two sheets
            this_data_dict = dict()
            self.data_dictionary[record_type] = this_data_dict
            this_explanation_dict = dict()
            self.variable_values_dictionary[record_type] = this_explanation_dict

            print "Processing '%s' Sheet" % thesheet.name
            print 
            # assuming this is fixed
            #variable_col = these_cols[variable_field]
            for row_num in range(thesheet.nrows):
                row = thesheet.row_values(row_num)
                this_var = row[self.variable_col]
                #print this_var
                if this_var == self.variable_field:
                    continue
                if this_var and any((x.strip() for x in row)):
                    #Row contains var and doesnt just consist of empties
                    if this_var not in this_data_dict:
                        this_variable_data_dict = dict()
                        this_data_dict[this_var] = this_variable_data_dict
                        for (field,col_num) in self.variable_properties_dict.iteritems():
                            val = row[col_num]
                            if field in ['BEG','LEN']:
                                try:
                                    val = int(val)
                                except ValueError:
                                    pass
                            this_variable_data_dict[field] = val
                    for file_type in self.file_types:
                        these_cols = self.variable_explanation_cols_dict[file_type]
                        if (this_var,file_type) not in this_explanation_dict:
                            this_variable_explanation_dict = dict()
                            this_explanation_dict[(this_var,file_type)] = this_variable_explanation_dict
                        else:
                            this_variable_explanation_dict = this_explanation_dict[(this_var,file_type)]
                        (low_str, hi_str, desc) = (row[these_cols[u'LO']],row[these_cols[u'HI']],
                                                   row[these_cols[u'VALUE DESCRIPTION']])
                        if hi_str:
                            this_variable_explanation_dict[(low_str,hi_str)] = desc
                        else:
                            this_variable_explanation_dict[low_str] = desc
        legal_vars = set([])
        self.legal_vars = legal_vars
        for (rt, var_dict) in self.data_dictionary.iteritems():
            for (var, var_info) in var_dict.iteritems():
                legal_vars.add((rt,var))


    def _get_db_val_ (self, row, field, record_type):
        """
        row:  the row string from the DB
        field: the field of the request, a string found in the VARIABLES
               col in the DB dictionary
        record_type: one of self.record_types:
               [u'Housing Unit Record',u'Person Record']

        Return None if record is not of correct type.
        """
        if self.check_row_record_type(record_type, row):
            var_info = self.data_dictionary[record_type][field]
            (start,offset) = (var_info['BEG']-1,var_info['LEN'])
            return row[start:start+offset]

    def __get_db_val__ (self, row, field, record_type):
        """
        row:  the row string from the DB
        field: the field of the request, a string found in the VARIABLES col in the DB dictionary
        record_type: one of self.record_types: [u'Housing Unit Record',u'Person Record']

        Returns None if record is not of correct type.

        Fastest version: Omit even row type check.  Do not use w/o
        some kind of prior checking.
        """
        global var_info0, record_type0, row0, field0
        #print 'hi'

        var_info = self.data_dictionary[record_type][field]
        (row0, var_info0,record_type0,field0) = row, var_info,record_type,field
        #print row, field, record_type
        (start,offset) = (var_info['BEG']-1,var_info['LEN'])
        return row[start:start+offset]

    def check_row_record_type (self, record_type, row):
        return self.record_type_dict[record_type] == row[0]
    
    def _cmp_db_val_ (self, row, field, record_type,query_val,op):
        """
        Assuming value is never None so we always return False
        for records of wrong type.
        """
        db_val = self.__get_db_val__(row,field,record_type)
        if type(query_val) == int:
            try:
                db_val = int(db_val)
            except ValueError:
                return False
        return op(db_val,query_val)
    
    def get_db_val (self, row, field, record_type):
        """
        row:  the row string from the DB
        field: the field of the request, an actual value in the VARIABLES col in the DB dictionary
        record_type: one of [u'Housing Unit Record',u'Person Record']

        """
        field = self.check_record_type_field(record_type,field)
        return self._get_db_val_ (row, field, record_type)


    def print_var_info (self, var0, record_type, file_type=None):

        if file_type is None:
            file_type = self.file_type

        new_var0 = self.check_record_type_field(record_type,var0)

        (var_info,var_values_info) = \
                 (self.data_dictionary[record_type][new_var0],self.variable_values_dictionary[record_type][(new_var0,file_type)])

        if var0 == new_var0:
            var_str = var0
        else:
            var_str = '%s => %s' % (var0, new_var0)

        print record_type, var_str, file_type, var_info
        field_len = var_info['LEN']
        print
        print ' ' * 4, var_info['DESCRIPTION']
        print ' ' * 4, len(var_info['DESCRIPTION']) * '='
        var_values = sorted(var_values_info.keys())
        for code in var_values:
            print ' ' * 4, '%*s  %s' % (field_len, code, var_values_info[code])
        print

    def open_db_file (self, db_file, file_type):
        assert file_type in self.file_types, '%s not one of recognized file types %s' % (file_type, self.file_types)
        self.file_type = file_type
        return open(db_file,'r')

    def check_specs(self, specs, record_type):
        """
        Currently record type must be the same for all specs in a spec list,
        since we can only check one row at a time.
        """
        res = []
        for spec in specs:
            (field,val,op) = decompose_spec(spec)
            field = self.check_record_type_field(record_type,field)
            assert val is not None, "Can't use None as a checkable DB value!"
            assert not(((op == operator.gt) or (op == operator.lt)) and type(val) == str),\
                   "Can only use inequality comparisons with numbers [not strings ('%s')]" % (val,)
            res.append((field,val,op))
        return res


    def check_record_type_field(self, record_type,field):
        assert record_type in self.record_types, '%s is not one of the known record types %s' % (record_type, self.record_types)
        try:
            assert (record_type,field) in self.legal_vars, '%s is not a known variable for a %s' % (field, record_type)
        except AssertionError as e:
            try:
               new_field = self.synonyms[field]
               assert (record_type,new_field) in self.legal_vars, \
                      '%s=>%s is not a known variable for a %s' % (field, new_field, record_type)
            except KeyError:
                #raise Exception, '%s is not a known variable or variable synonym for a %s' % (field, record_type)
                raise e
            field = new_field
        return field
        
        
    def select_rows (self, specs, record_type, db_file, file_type, op=operator.eq,verbose=False):
        """
        Specs is a list of field, val_op pairs.  A val_op is a 
        Return a list containing each line in db file that satisfies ALL specs (conjunctive interp of specs).

        Optional operator spec.  Currently same operator and same record_type applies to all specs, for simplicity.

        """
        new_specs = self.check_specs(specs,record_type)
        row_ctr = self.reset_event_counter('rows')
        with self.open_db_file(db_file,file_type) as fh:
            res = []
            for line in fh:
                line = line.strip()
                if self.check_row_record_type (record_type, line):
                    ##  Do record check first, counting only records of right type as query rows, for
                    ## more  sensible percentage calculations.
                    row_ctr['rows'] += 1
                    if all((self._cmp_db_val_ (line, field, record_type,val, op) for (field, val, op) in new_specs)):
                        res.append(line)
        if verbose:
            self.print_result_summary(specs,res,record_type)
        return res


    def reset_event_counter (self,event_type):
        if hasattr(self, 'event_counter'):
            self.event_counter[event_type] = 0
            return self.event_counter
        else:
            ctr = Counter()
            self.event_counter = ctr
            return ctr

    def print_result_summary(self, specs,res,record_type):
        spec_strs = self.__get_spec_print_strs__(specs)
        max_width = max(map(len, spec_strs))
        print '% *s  %.4f%% of the %ss' % (max_width, spec_strs[0], 100*(float(len(res))/self.event_counter['rows']),record_type)
        for ss in spec_strs[1:]:
            print '%s  ' % ss
        print

    def __get_spec_print_strs__ (self,specs):
        specs = [decompose_spec(spec) for spec in specs]
        fields_width = max(map(len, [ss[0] for ss in specs]))
        ops_width = max(map(len, [ss[2].__name__ for ss in specs]))
        vals_width = max(map(len, [str(ss[1]) for ss in specs]))
        return ['%*s  %*s  %*s' % (fields_width, field, ops_width, op.__name__, vals_width, val) for (field, val,op) in specs]

    def get_internal_field_name (self, name):
        try:
            return self.synonyms[name]
        except KeyError:
            return name
        

    def get_sample_record_from_db (self,db_file):
        print 'Opening %s' % (db_file,)
        with open(db_file,'r') as fh:
            line = fh.readline().strip()
            line = fh.readline().strip()
        print line
        return line
        

class DataFrameWrapper (object):
    """
    Usage conventions
    =================
    Uses a data model such as the
    CensusInfo instance called C{ci} in the examples
    below.  Then use this class to
    make a Python internal rep of the actual parts of a PUMS DB.

    >>> ci = CensusInfo (xl_file)
    >>> df = DataFRameWrapper(ci)
    >>> df.fill_frame ([], ci.person_record, one_percent_data_file, ci.one_percent_file, \
                   'race','education','income','gender','age','relationship')

    C{df} now contains a Python list of lists rep of the relevant info from all
    the rows. First arg (an empty list) here, can be any list of query specs of
    the sort used by the CensusInfo C{select_rows} method, for subsetting the
    census DB.

    Choose a filename and then save,
    
    >>> one_percent_extract = os.path.join(one_percent_dir,'census.dat')
    >>> df.save_frame(one_percent_extract)

    The file is in a format suitable for reading into either a pandas or R dataframe.


    With default comma separator, read this into R with the R command::
    
     > df <- read.table('census.dat',header=TRUE,sep=",")
     > str(df)
     'data.frame':	44487 obs. of  7 variables:
     $ serial_no   : int  117 117 117 127 127 127 127 134 195 195 ...
     $ race        : int  47 47 47 47 47 47 47 47 47 47 ...
     $ education   : int  12 12 0 5 10 5 7 13 10 9 ...
     $ income      : int  47300 24300 NA 0 22000 0 5000 13000 0 0 ...
     $ gender      : int  1 2 1 1 2 1 1 2 1 2 ...
     $ age         : int  31 25 0 22 35 19 18 23 56 56 ...
     $ relationship: int  1 2 3 1 2 5 17 1 1 2 ...


    Read into pandas (in Python) with::
      >>> import pandas as pd
      >>> pd.read_csv('census.dat', header=True)
      
    """

    def __init__ (self, data_model):
        """
        data model is a CensusInfo inst
        """

        self.data_model = data_model
        self.file_type = data_model.file_type

    def fill_frame (self, query_specs, record_type, db_file, file_type, *col_specs):
        col_specs = list(col_specs)
        db_extract = self.data_model.select_rows(query_specs,record_type,db_file,file_type)
        ## serial_no can be used to connect everything, always include
        col_specs = self.add_key_to_col_specs(col_specs)
        (self.header,self.col_dict,self.rows) = (col_specs,dict(),[])
        for db_row in db_extract:
            row = []
            self.rows.append(row)
            for (i,var) in enumerate(col_specs):
                self.col_dict[var] = i
                var = self.data_model.check_record_type_field(record_type,var)
                row.append(self.data_model.__get_db_val__(db_row,var,record_type))


    def make_complete_frame(self,record_type, db_file, file_type):

        col_names = self.data_model.data_dictionary[record_type].keys()
        return self.fill_frame([],record_type, db_file, file_type,
                               *col_names)


    def add_key_to_col_specs(self,col_specs):
        nice_key = self.data_model.key
        key = self.data_model.synonyms[nice_key]
    
        if (key not in col_specs) and (nice_key not in col_specs):
            return [nice_key] + col_specs
        else:
            try:
                key_index = col_specs.index(key)
            except ValueError:
                key_index = col_specs.index(nice_key)
            return [nice_key] + \
                   col_specs[:key_index] + \
                   col_specs[key_index+1:]
        

    def save_frame (self, filename,header=True,separator=','):
        """
        Writes a file in a format readable as a dataframe by R or pandas.

        Read this into pandas with the command::

        df = pd.read_csv(filename)

        Read this into R with the command::
        df <- read.table('census.dat',header=TRUE,sep=",")
        """
        if header:
            rows = [self.header]
            rows.extend(self.rows)
        else:
            rows = self.rows
        with open(filename,'w') as ofh:
            for row in rows:
                # Get rid of space padding to satisfy data size
                row = [e.strip() for e in row]
                print >> ofh, separator.join(row)

    ### Panda related methods

    def make_subtable (self, cols, filename=None):
        """
        *** Not yet implemented ***
        
        cols is the list of cols to include in the save subtable.

        cols = ['race','education','income','gender','age','relationship']

        Not yet implemented (NYI)
        """

        # select out a subset of the cols
        cols = self.add_key_to_col_specs(cols)        
        internal_cols = [self.data_model.get_internal_field_name(col) \
                         for col in cols]

        for row in self.rows:
            # NYI
            pass

        self.header = cols
        self.internal_cols = internal_cols
        self.col_dict = dict(zip(cols, range(len(cols))))



        return subtable



    def panda_save_subtable (self, subtable, filename,
                             header=None, index=False):
        """
        subtable IS a pandas data frame.  This is just a wrapper for
        the pandas method.
        """
    

        subtable.to_csv(filename,
                        header=header,
                        index=index)


if __name__ == '__main__':

    import os.path


    ###############################################################
    ###############################################################
    #
    #  Files with Census DB and Census DB data model
    #
    ###############################################################
    ###############################################################
    
    working_dir = '/Users/gawron/ext/src/Rdata/USCensusData2000'
    one_percent_dir = os.path.join(working_dir, 'pums_1percent')
    five_percent_dir = os.path.join(working_dir, 'pums_5percent')
    one_percent_data_file = os.path.join(one_percent_dir,'revisedpums1_alabama_01.txt')
    five_percent_data_file = os.path.join(five_percent_dir,'revisedpums5_Alabama_01.txt')
    data_model_file = os.path.join(five_percent_dir, '5%_PUMS_record_layout.xls')

    
    ###############################################################
    ###############################################################
    #
    #  Make Data model object
    #
    ###############################################################
    ###############################################################

    # read i Excel file with data model

    ci = CensusInfo (data_model_file)
                                                          
    ###############################################################
    ###############################################################
    #
    #  Data Model examples
    #
    ###############################################################
    ###############################################################

    ci.print_var_info('RELATE',ci.record_types[1],ci.file_types[1])


    line = ci.get_sample_record_from_db (one_percent_data_file)

    file_type = ci.file_types[1]

    (var0,record_type, file_type) = ('RELATE',ci.record_types[1],file_type)
    val = ci.get_db_val(line, var0, record_type)
    var_values_info = ci.variable_values_dictionary[record_type][(var0,file_type)]

    print var0,  record_type, val, var_values_info[val]

    #ci.print_var_info ('EDUC', ci.person_record)
    ci.print_var_info ('education', ci.person_record)
    #ci.print_var_info ('RACE3', ci.person_record)
    ci.print_var_info ('race', ci.person_record)
    ci.print_var_info ('income', ci.person_record)

    # Common to both records, links them

    ci.print_var_info('serial_no',ci.household_record)

    ci.print_var_info('serial_no',ci.person_record)

    ci.print_var_info('num_persons',ci.household_record)

    try:
        ci.print_var_info('num_persons',ci.person_record)
    except Exception as e:
        print e.message

    ################################################################
    ################################################################
    #
    #
    #      Making data frame (no pandas)
    #
    ################################################################
    ################################################################

    #df = DataFRameWrapper(ci)
    #### Norm Matloff's example
    #df.fill_frame ([], ci.person_record, one_percent_data_file, ci.one_percent_file, \
    #               'race','education','income','gender','age','relationship')

    ################################################################
    ################################################################
    #
    #   C r e a t e d   D a t a     F i l e s  
    #
    ################################################################
    ################################################################

    #one_percent_extract = 'revisedpums1_alabama_01_race_education_income_gender_age_relationship_with_headers.dat'
    one_percent_subtable = 'revisedpums1_alabama_01_race_education_income_gender_age_relationship_with_headers.dat'
    one_percent_extract = 'revisedpums1_alabama_01_complete_with_headers.dat'

    ################################################################
    ################################################################
    #
    #   G e t t i n g   a   C o m p l e t e    DB   E x t r a c t
    #
    ################################################################
    ################################################################

    

    #ci = CensusInfo (xl_file)
    #df = DataFRameWrapper(ci)
    #df.make_complete_frame(ci.person_record,one_percent_data_file,ci.one_percent_file)

    

    ################################################################
    ################################################################
    #
    #   S a v i n g   
    #
    ################################################################
    ################################################################

    #  Save current df frame to a file, csv format.
    #  Only needs to be done once.  From then use pd.read_csv
    #df.save_frame(os.path.join(one_percent_dir,one_percent_extract))

    ################################################################
    ################################################################
    #
    #   U s i n g    P a n d a [reading in  data, making a subtable]
    #
    ################################################################
    ################################################################
    
    # Read complete person records one percent db for alabama
    # in to a panda dataframe.
    
    p_df = pd.read_csv(os.path.join(one_percent_dir, one_percent_extract))
    
    # Person records for One household
    hhld_117_person_recs = p_df[p_df['SERIALNO']==117]

