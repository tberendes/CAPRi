"""
Authors: Pooja Khanal, Todd Berendes
Module to formulate the query with the filters.
This module acts as a helper to the corresponding lambda_function.py

TAB 3/28/23 modified for V7 GPM data, generalized based on schema,
implemented boolean parameter clause
"""

import os
import json
import datetime
from environment import *
from get_schema import *

# special parameters for datetime query, breaks down to year,month,date query
datetime_parameters = {
    "datetime": ["max_datetime", "min_datetime"]
}

# Parameters which will be queried to have either outer coverage range or inner coverage range
# Assumption : For the below parameters , there exists another swath parameter, for eg: swath="outer" or swath = "inner" for rayNum
swath_parameters = {
    "raynum": ["start_raynum", "end_raynum"]
}

# greater_than_equal_to, less_than_equal_to list for numeric value comparision
comparators_gte_lte = [">=", "<="]

# like, not_like list for substring matching
comparators_like_not_like = ["LIKE", "NOT LIKE"]

# less_than, greater_than list for outer swath calculation
comparators_gt_lt = ["<", ">"]

# comparators used to compute the relative difference between two values
difference_comparators = {"gte": ">=", "lte": "<=", "lt": "<", "gt": ">", "eq": "="}


def get_query(event, columns_array):
    # description: function to formulate the query. The query is then used to create a filter in the Athena database.
    # parameters: takes the event of the lambda as its parameter
    # return: formulated query is returned to the main lambda_function

    # list of sql clauses derived from the parameters to filter the SQL query. SELECT * FROM <table_name> WHERE <filter_clauses>
    filter_clauses = []
    query_list = []
    # append white space before and after the operators to separate operator from operands
    AND = " " + "AND" + " "
    OR = " " + "OR" + " "

    table = event['table_name']

    def append_sql_filter_clauses(parameters, comparator, operator=AND):
        # description: Appends the filter clause list for each parameter on the given parameters' dictionary
        # parameters: parameters are min-max parameters, like-not-like parameters, swath parameters. Each of them has their own set of dictionary
        # return: none; makes changes to the global variable

        for parameter, parameter_values in parameters.items():
            parameter_clause = []  # clause list for a single parameter
            for i in range(len(parameter_values)):
                if parameter_values[i] in event:
                    parameter_value = event[parameter_values[i]]
                    clause = get_parameter_filter_clause(parameters, parameter_value, parameter, comparator[i])
                    parameter_clause.append(clause)

            if parameter_clause:
                operator = " " + operator
                parameter_clause = operator.join(parameter_clause)
                filter_clauses.append("({0})".format(parameter_clause))

    def get_parameter_filter_clause(parameters, parameter_value, parameter, comparator):

        # description:- Gets filter clause for a single parameter. This is used to compound all the parameters and is used in append_sql_filter_clauses()
        # parameters:- parameters are inidvidual dictionary for a parameter type. parameter value: the value of the parameter which is to be added to the compound clause
        # return:- returns the clause for this individual parameter.

        if parameters == like_not_like_parameters:
            compound_clause = get_multi_value_parameter_filter_clause(parameter_value, parameter, comparator)
            clause = "({})".format(compound_clause)

        elif parameter == "datetime":
            # get datetime clause
            clause = get_date_time_clause(parameter_value, comparator)

        elif parameters == min_max_parameters:
            clause = parameter + " " + comparator + " " + str(parameter_value)

        return clause

    # need to handle string values for year, month, day for cases when partitioning is used (partitions seem to be string values)
    def get_date_time_clause(parameter_value, comparator):
        # parameters : datetime
        # parameter_value : min_datetime, max_datetime etc.
        # parameter : time
        # comparators : gt_lt

        value_datetime = datetime.datetime.strptime(parameter_value,
                                                    "%Y-%m-%d %H:%M:%S")  # min_datetime or max_datetime
        year = value_datetime.strftime("%Y")
        month = value_datetime.strftime("%m")
        day = value_datetime.strftime("%d")
        time = value_datetime.strftime("%H:%M:%S")
        clause = "(year {4} {0} OR (year = {0} AND (month {4} {1} OR month = {1} AND (day {4} {2} OR (day = {2} AND (time {4}= '{3}' ))))))".format(
            year, month, day, time, comparator)
        return clause

    def get_multi_value_parameter_filter_clause(clause, parameter, comparator):
        # description:- For OR'ing or AND'ing if we get a list of multiple values for same parameter/field eg : sensor_like= L%,K% ; sensor_not_like=%R,%K
        # parameters:- clause is the list of values for a single parameter, parameter is the parameter's name and comparator is the type of comparator suitable for this parameter
        # return:- returns the compund clause created for an individual parameter

        clause = clause.split(',')
        multi_value_parameter_clause = []
        for each in clause:
            each = "'{0}'".format(each)
            multi_value_parameter_clause.append(parameter + " " + comparator + " " + each)
        # For list of substrings to include, we join them using OR. eg: sensor_like=L%,k% gives either sensor_like = L% OR sensor_like= K%
        # For list of substrings to exclude, we join them using AND. eg: senor_not_like=%R,%K gives sensor_not_like=%R AND sensor_not_like =%K
        compound_clause = (OR if comparator == "LIKE" else AND).join(multi_value_parameter_clause)
        return compound_clause

    # for generic parameters
    def generic(params, step, operator=difference_comparators):
        # description:- Generic computation by grouping parameters
        # parameters:- params is the set of parameters to pass. step is the number of items required to formulate the group. operator is the set of operator which acts on respective parameter set.
        # return:- none, performed in global filter_clause
        # example:-
        # "min": "latitude,45.2,longitude,60" gives latitude >= 45.2 and longitude >= 60 ,
        # "max": "latitude,45.5,longitude,65" gives latitude <=45.5 and longitude <= 65,
        # "difference": "topheight,lt,ruc_0_height,4,bottomheight,gte,ruc_0_height,5,hid1,eq,hid2,6 gives topheight - ruc_0_height < 4 AND bottomheight - ruc_0_height >= 5 AND hid1 - hid2 = 6"

        values = event[params]
        values = values.split(',')

        n = len(values)

        def group(i):
            # description:- to group the parameters , eaither by 2 or 4
            # parameters:- i to take the position of the parameters
            # return:- min, max and difference clause after grouping
            if params == "difference":
                clause = values[i] + ' - ' + values[i + 2] + ' ' + difference_comparators[values[i + 1]] + ' ' + values[
                    i + 3]
            else:  # params == "min" or params == "max"
                if values[i].strip() == "datetime":
                    if params == "min":
                        comparator = difference_comparators["gt"]
                    elif params == "max":
                        comparator = difference_comparators["lt"]
                    values[i + 1] = values[i + 1].strip()
                    clause = get_date_time_clause(values[i + 1], comparator)

                else:
                    clause = values[i] + difference_comparators[operator] + values[i + 1]
            return clause

        for i in range(0, n, step):
            clause = group(i)
            filter_clauses.append(clause)

    if 'difference' in event:
        generic('difference', 4)

    if 'min' in event:
        generic('min', 2, 'gte')

    if 'max' in event:
        generic('max', 2, 'lte')

    # need to get list of columns from the API, then loop through

    # boolean parameters
    for parameter in boolean_parameters:
        if parameter + '_true' in event:
            clause = parameter + ' IS TRUE '
            filter_clauses.append(clause)
        elif parameter + '_false' in event:
            clause = parameter + ' IS FALSE '
            filter_clauses.append(clause)

    # parameters to be queried by specific values , eg: latitude =90
    for parameter in all_parameters:
        if parameter in event:
            val = event[parameter]
            clause = parameter + ' = ' + str(val)
            filter_clauses.append(clause)

    # Appends to SQL filter clauses
    append_sql_filter_clauses(min_max_parameters, comparators_gte_lte)
    append_sql_filter_clauses(like_not_like_parameters, comparators_like_not_like)

    if "swath" in event and event["swath"] == "outer":
        append_sql_filter_clauses(swath_parameters, comparators_gt_lt, OR)
    else:
        append_sql_filter_clauses(swath_parameters, comparators_gt_lt)

    append_sql_filter_clauses(datetime_parameters, comparators_gt_lt)

    def query_builder(fields):
        # description: To build query from the given fields
        # parameters: string of fields
        # return: query
        if filter_clauses:
            query = 'SELECT ' + fields + ' FROM ' + table + ' WHERE ' + AND.join(filter_clauses)
        else:
            query = 'SELECT ' + fields + ' FROM ' + table

        return query

    for each in columns_array:
        fields = ",".join(each)
        query = query_builder(fields)
        query_list.append(query)
    return query_list



