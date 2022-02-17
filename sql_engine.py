import sys
import csv
from collections import OrderedDict

listOfTables = OrderedDict({})
# dict{tbl_nm: dict{col : list(elems)}}

def readMetaData():

	# print('f: readMetaData')

	f = open('metadata.txt', 'r')

	for line in f:

		line = line.strip()

		if line == "<begin_table>":

			tbl_schema = OrderedDict({})
			cnt = 1

			for col_nm in f:

				col_nm = col_nm.strip()

				if col_nm == "<end_table>":
					break

				if cnt == 1:
					tbl_nm = col_nm
					cnt = 0
				else:
					tbl_schema[col_nm] = []

		listOfTables[tbl_nm] = tbl_schema


def loadDS(fileNames):

	# print('f: loadDS')

	for fname in fileNames:

		try:
			tempSchema = listOfTables[fname]
		except KeyError:
			sys.exit("Invalid table name")

		csvname = fname+'.csv'

		#need to handle table not found exception
		with open(csvname, 'r') as f:
			reader = csv.reader(f)

			for row in reader:
				i = 0
				for col in tempSchema:
					tempSchema[col].append(int(row[i]))
					i += 1

		listOfTables[fname] = tempSchema

	# print(listOfTables)


def disp_res(result):

	for l in result:
		for x in l:
			print(x, end='\t')
		print()



def singleTableCols(cols, tbls, resultTable):

	# single table without where clause
	# print('f: singleTableCols')

	if cols == '*':

		# get all columns of a single table without where clause

		tempTable = resultTable[tbls]
		tempList = list(tempTable.keys())
		no_of_rows = len(tempTable[tempList[0]])

		for col in tempTable:
			print(col, end='\t')
		print()

		for i in range(no_of_rows):
			for col in tempTable:
				print(tempTable[col][i], end='\t')
			print()

	else:

		# get selected columns of a single table without where clause
		# check for aggregate queries
		a = ['max(', 'min(', 'sum(', 'avg(']

		if any(x in cols for x in a):

			# aggreagate functions
			agg_col = cols[cols.find('(')+1:cols.find(')')]
			print(agg_col)

			if 'max' in cols:
				print(max(resultTable[tbls][agg_col]))
			elif 'min' in cols:
				print(min(resultTable[tbls][agg_col]))
			elif 'sum' in cols:
				print(sum(resultTable[tbls][agg_col]))
			elif 'avg' in cols:
				print(sum(resultTable[tbls][agg_col])/len(resultTable[tbls][agg_col]))

		elif 'distinct ' in cols:

			result = [[]]
			listofcols = cols[cols.find('distinct')+8:]
			listofcols = listofcols.split(',')

			temp = []
			for x in listofcols:
				temp.append(x.strip())
			listofcols = temp

			tempTable = resultTable[tbls]
			tempList = list(tempTable.keys())
			no_of_rows = len(tempTable[tempList[0]])

			for i in range(no_of_rows):
				temp = []
				for x in listofcols:
					temp.append(tempTable[x][i])
				if temp not in result:
					result.append(temp)

			for col in listofcols:
				print(col, end='\t')

			disp_res(result)

		elif 'count(' in cols:

			agg_col = cols[cols.find('(')+1:cols.find(')')]
			print(agg_col)
			print(len(resultTable[tbls][agg_col]))

		else:

			# get selected columns of a single table without where clause

			listofcols = cols.split(',')
			for x in listofcols:
				print(x.strip(), end='\t')
			print()

			tempTable = resultTable[tbls]
			tempList = list(tempTable.keys())
			no_of_rows = len(tempTable[tempList[0]])

			for i in range(no_of_rows):
				for col in tempTable:
					if col in cols:
						print(tempTable[col][i], end='\t')
				print()


def singleTableWhere(cols, tbls, whr_cls):

	resultSchema = OrderedDict({})
	resultSchema['res'] = listOfTables[tbls]

	list_of_whr_cls = whr_cls.split('and')

	for whr_cond in list_of_whr_cls:

		res_idx = []
		whr_list = whr_cond.split()

		if len(whr_list) != 3:
			a = ['>', '<', '>=', '<=', '=']
			cnt = 0
			for x in a:
				if x in whr_cond:
					op = x
					whr_list = whr_cond.split(x)
					val = int(whr_list[1].strip())
					col_nm = whr_list[0].strip()
					break
				cnt += 1

			if cnt == 5:
				sys.exit('Invalid where clause')
		else:
			op = whr_list[1].strip()
			val = int(whr_list[2].strip())
			col_nm = whr_list[0].strip()

		tempCol = resultSchema['res'][col_nm]

		if op == ">":
			#greater than

			i = 0
			for x in tempCol:
				if x > val:
					res_idx.append(i)
				i += 1

		elif op == "<":
			#less than

			i = 0
			for x in tempCol:
				if x < val:
					res_idx.append(i)
				i += 1

		elif op == "=":
			#equal to

			i = 0
			for x in tempCol:
				if x == val:
					res_idx.append(i)
				i += 1

		elif op == ">=":
			#equal to

			i = 0
			for x in tempCol:
				if x >= val:
					res_idx.append(i)
				i += 1

		elif op == "<=":
			#equal to

			i = 0
			for x in tempCol:
				if x <= val:
					res_idx.append(i)
				i += 1

		tempTable = resultSchema['res']

		for col in tempTable:
			tempList = tempTable[col]
			lst = []
			for rn in res_idx:
				lst.append(tempList[rn])
			resultSchema['res'][col] = lst

	singleTableCols(cols, 'res', resultSchema)


def multiTablesWhere(cols, tbls, whr_cls):

	whr_list = whr_cls.split()

	if len(whr_list) == 3:
		opr = whr_list[1].strip()
		l_opd = whr_list[0].strip()
		r_opd = whr_list[2].strip()
	else:
		sys.exit('Unable to handle this query :(')
		
	ldts = l_opd.split('.')
	rdts = r_opd.split('.')

	l_tbl = ldts[0]
	l_col = ldts[1]

	r_tbl = rdts[0]
	r_col = rdts[1]

	l_list = listOfTables[l_tbl][l_col]
	r_list = listOfTables[r_tbl][r_col]

	i = 0
	res_list = []

	for x in l_list:

		try:
			# pending: need to handle if there are repeated values in columns
			r_idx = r_list.index(x)
			res_list.append([i, r_idx])

		except ValueError:
			pass

		i = i+1

	resultSchema = OrderedDict({})
	resultSchema['res'] = OrderedDict({})
	lTable = listOfTables[l_tbl]
	rTable = listOfTables[r_tbl]

	# print(res_list)

	if len(res_list) == 0:
		sys.exit('No matching found between the columns')

	for col in lTable:
		temp = []
		col_nm = l_tbl+'.'+col
		# if col_nm == l_opd:
		# 	continue
		for lst in res_list:
			temp.append(lTable[col][lst[0]])

		resultSchema['res'][col_nm] = temp

	for col in rTable:
		temp = []
		col_nm = r_tbl+'.'+col

		for lst in res_list:
			temp.append(rTable[col][lst[1]])

		resultSchema['res'][col_nm] = temp

	# to remove duplicate column
	del resultSchema['res'][l_opd]
	singleTableCols(cols, 'res', resultSchema)


def queryEvaluation(cols, tbls, whr_cls):

	# print('f: queryEvaluation')

	if tbls.find(',') != -1:

		# multiple tables

		list_of_tbls = tbls.split(',')
		lst_tbls = []

		for tbl in list_of_tbls:
			lst_tbls.append(tbl.strip())

		loadDS(lst_tbls)

		multiTablesWhere(cols, lst_tbls, whr_cls)

	else:
		# single table
		loadDS([tbls])

		if not whr_cls:
			# single table without where clause

			singleTableCols(cols, tbls, listOfTables)

		else:
			# single table with where clause

			if whr_cls.find(' and ') != -1:
				# single table with multiple where clause
				singleTableWhere(cols, tbls, whr_cls)

			else:
				# single table with one where clause
				singleTableWhere(cols, tbls, whr_cls)


def parseQuery():

	#print('f: parseQuery')

	# print('No.of args:', len(sys.argv))
	# print('List:', str(sys.argv))
	query = sys.argv[1]
	print('Query:', query)

	if query.find('select ') == 0:

		frm = query.find(' from ')

		if frm != -1:

			cols = query[7:frm]
			whr_cls = query.find(' where ')

			if whr_cls != -1:
				tbls = query[frm+6:whr_cls]
				whr_cls = query[whr_cls+7:]
			else:
				if query.find('where') != -1:
					sys.exit('Invalid Query')
				else:
					tbls = query[frm+6:]
					whr_cls = ''

		else:
			sys.exit('Invalid Query')

	else:
		sys.exit('Invalid Query')

	queryEvaluation(cols, tbls, whr_cls)

if __name__ == "__main__":
	readMetaData()
	parseQuery()


# print('cols:', cols)
# print('tbls:', tbls)
# print('whr_cls:', whr_cls)
