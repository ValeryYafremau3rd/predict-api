import xlsxwriter

def build_xml():
    workbook = xlsxwriter.Workbook('static/' + 'hello.xlsx')
    worksheet = workbook.add_worksheet()
    worksheet.write('A1', 'Hello world')
    workbook.close()
    return 'static/' + 'hello.xlsx'