"""
cse6242 s22
wrangling.py - utilities to supply data to the templates.

This file contains a pair of functions for retrieving and manipulating data
that will be supplied to the template for generating the table. """
import csv

def username():
    return 'jzhu398'

# take second element for sort
def takeThird(elem):
    return float(elem[2])

def data_wrangling():
    with open('data/movies.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        table = list()
        # Feel free to add any additional variables
        count = 0
        
        # Read in the header
        for header in reader:
            break
        
        # Read in each row
        for row in reader:
            table.append(row)

            # Only read first 100 data rows - [2 points] Q5.a
            count = count + 1
            if count == 100:
                break
        
        # Order table by the last column - [3 points] Q5.b
        # refer to https://www.programiz.com/python-programming/methods/list/sort for using sort
        table.sort(key = takeThird, reverse = True)
    
    return header, table

