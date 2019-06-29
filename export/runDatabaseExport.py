import DataBaseExport
import ExportToCsv

if __name__ == "__main__" :
    print("启动进程")
    mysql = DataBaseExport.MySQL()
    print("启动excel")
    csv = ExportToCsv.Csv()
    print("初始化完成，链接数据库")
    mysql.connection()
    print("保存迭代器")
    csv.set_itorater(mysql.ReadData)
    mysql.start()
    csv.start()
