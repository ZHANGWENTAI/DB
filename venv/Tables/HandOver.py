from decimal import *
class HandOver:
    def __init__(self, cursor):
        self.filePath = 'C:\\Users\\dxxhj\\Desktop\\DBData\\11.tbHandOver.csv'
        self.itemList = []
        self.cnt = 0
        self.cursor = cursor
        self.del_trig = """
        IF EXISTS (select * from sys.triggers where name='trig_handover')
        BEGIN
            DROP TRIGGER trig_handover
        END
        """
        self.trig = """
        CREATE trigger trig_handover
        ON handover
        INSTEAD OF INSERT
        AS
        BEGIN
        IF EXISTS(SELECT city FROM inserted)  --插入和更新
            DECLARE cur CURSOR forward_only 
            FOR SELECT * FROM inserted
            OPEN cur  --打开游标
            DECLARE @city NVARCHAR(255)
            DECLARE @scell_id VARCHAR(50)
            DECLARE @ncell_id VARCHAR(50)
            DECLARE @hoatt INT
            DECLARE @hosucc INT
            DECLARE @hosuccrate FLOAT
            FETCH NEXT FROM cur INTO @city, @scell_id, @ncell_id, @hoatt, @hosucc, @hosuccrate
            WHILE(@@FETCH_STATUS=0) 
            BEGIN 
                --增加操作
                IF (@hosuccrate='3.1415926') BEGIN SET @hosuccrate=null END 
        	    IF ((SELECT COUNT (*) FROM handover WHERE scell_id=@scell_id AND ncell_id=@ncell_id)<1)
        	    BEGIN
        	        INSERT INTO handover VALUES (@city, @scell_id, @ncell_id, @hoatt, @hosucc, @hosuccrate)
        	    END
        	    ELSE
        	    BEGIN
        	        UPDATE handover SET 
        	        city=@city,
        	        hoatt=@hoatt, 
        	        hosucc=@hosucc, 
        	        hosuccrate=@hosuccrate
        	        WHERE scell_id=@scell_id AND ncell_id=@ncell_id
        	    END
                FETCH NEXT FROM cur INTO @city, @scell_id, @ncell_id, @hoatt, @hosucc, @hosuccrate   --指向下一条
            END 
            CLOSE cur  --关闭游标
            DEALLOCATE cur  --销毁游标资源
        END 
        """
        self.cursor.execute(self.del_trig)
        self.cursor.execute(self.trig)

    def loadData(self, size):
        city = ""
        sCellID = ""
        nCellID = ""
        hoatt = 0
        hosucc = 0
        hosuccRate = 0
        with open(self.filePath) as file_obj:
            for content in file_obj:
                if self.cnt == size:
                    self.insertAll()
                    self.itemList.clear()
                    self.cnt = 0
                dataList = content.rstrip().split(",")
                city = dataList[0]
                sCellID = dataList[1]
                nCellID = dataList[2]
                hoatt = dataList[3]
                hosucc = dataList[4]
                hosuccRate = dataList[5]
                if hosucc == '':
                    hosucc = '0'
                if hoatt == '':
                    hoatt = '0'
                if hosuccRate == '':
                    if hoatt == '0':
                        hosuccRate = '3.1415926'
                    else:
                        hosuccRate = float(int(hosucc) / int(hoatt))
                tempSqlItem = (city, sCellID, nCellID, hoatt, hosucc, hosuccRate)
                if self.isOK(tempSqlItem):
                    self.itemList.append(tempSqlItem)
                    self.cnt += 1
            if self.cnt != 0:
                self.insertAll()
                self.itemList.clear()

    def isOK(self, item):
        if item[3] == '0':
            if item[5] != '3.1415926':
                print("handover:hoatt=0,hosuccRate!=null")
                return False
        else:
            if float(item[5]) != float(Decimal(str(int(item[4]) / int(item[3])))
                                               .quantize(Decimal('0.0000'),rounding=ROUND_HALF_UP)):
                print("--------------------------------------------------")
                print(item[5])
                print(Decimal(str(int(item[4]) / int(item[3]))).quantize(Decimal('0.0000'),rounding=ROUND_HALF_UP))
                print(item)
                print("handover:hosuccRate!=hosucc/hoatt")
                return False
        return True

    def insertAll(self):
        script = 'INSERT INTO handover VALUES(?,?,?,?,?,?)'
        self.cursor.executemany(script, self.itemList)
        self.cursor.commit()