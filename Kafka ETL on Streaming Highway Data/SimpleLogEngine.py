from datetime import datetime 


def log(message):
	"""
	Simple logging function that manually writes to a CSV log file instead of using Python's logging function.
	Log headers for CSV format is: timestamp, function, input, output, and file path.

	Parameters
	__________
	message: str
		Should be formatted as function name being called, input value or None, output or None, and current
		file path of script utilizing the function (the __file__ Python variable contains current file path). 
		E.g: 'function_name,input_value,None,C://directory1/directory2/file.py'
	"""
	with open ('highway-traffic-project-log.csv', 'a') as loggy:
		loggy.write(datetime.now().strftime('%H:%M:%S-%B-%d-%Y') + ',' + message + '\n') # \n for newline after log is added.


#log(f'function_test,value_here,None,{__file__}') # for testing.