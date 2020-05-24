class ENodeB:
    def __init__(self, cursor):
        self.filePath = 'C:\\Users\\dxxhj\\Desktop\\DBData\\1.tbCell.csv'
        self.itemList = []
        self.cnt = 0
        self.cursor = cursor
        self.del_trig = """
        IF EXISTS (select * from sys.triggers where name='trig_enodeb')
        BEGIN
            DROP TRIGGER trig_enodeb
        END
        """
        self.trig = """
        CREATE trigger trig_enodeb
        ON enodeb
        INSTEAD OF INSERT
        AS
        BEGIN
        IF EXISTS(SELECT enodebid FROM inserted)  --插入和更新
            DECLARE cur CURSOR forward_only 
            FOR SELECT city, enodebid, enodeb_name, vendor, longitude, latitude, style FROM inserted
            OPEN cur  --打开游标
            DECLARE @city NVARCHAR(255)
            DECLARE @enodebid INT
            DECLARE @enodeb_name NVARCHAR(255)
            DECLARE @vendor NVARCHAR(255)
            DECLARE @longitude FLOAT
            DECLARE @latitude FLOAT
            DECLARE @style NVARCHAR(255)
            FETCH NEXT FROM cur INTO @city, @enodebid, @enodeb_name, @vendor, @longitude, @latitude, @style
            WHILE(@@FETCH_STATUS=0) 
            BEGIN 
                --增加操作
        	    IF ((SELECT COUNT (*) FROM enodeb WHERE enodebid=@enodebid)<1)
        	    BEGIN
        	        INSERT INTO enodeb VALUES (@city, @enodebid, @enodeb_name, @vendor, @longitude, @latitude, @style)
        	    END
        	    ELSE
        	    BEGIN
        	        UPDATE enodeb SET 
        	        city=@city,
                    enodeb_name=@enodeb_name, 
                    vendor=@vendor, 
                    longitude=@longitude, 
                    latitude=@latitude, 
                    style=@style
        	        WHERE enodebid=@enodebid
        	    END
                FETCH NEXT FROM cur INTO @city, @enodebid, @enodeb_name, @vendor, @longitude, @latitude, @style   --指向下一条
            END 
            CLOSE cur  --关闭游标
            DEALLOCATE cur  --销毁游标资源
        END 
        """
        self.cursor.execute(self.del_trig)
        self.cursor.execute(self.trig)

    def loadData(self, size):
        city = ""
        eNodeBID = 0
        eNodeBName = ""
        vendeor = ""
        longitude = 0
        latitude = 0
        style = ""
        with open(self.filePath) as file_obj:
            for content in file_obj:
                if self.cnt == size:
                    self.insertAll()
                    self.itemList.clear()
                    self.cnt = 0
                dataList = content.rstrip().split(",")
                city = dataList[0]
                eNodeBID = dataList[3]
                eNodeBName = dataList[4]
                vendor = dataList[10]
                longitude = dataList[11]
                latitude = dataList[12]
                style = dataList[13]
                tempSqlItem = (city, eNodeBID, eNodeBName, vendor, longitude, latitude, style)
                if self.isOK(tempSqlItem):
                    self.itemList.append(tempSqlItem)
                    self.cnt += 1
            if self.cnt != 0:
                self.insertAll()
                self.itemList.clear()

    def isOK(self, item):
        if item[3] not in ["华为", "中兴", "诺西", "爱立信", "贝尔", "大唐"]:
            print("ENodeB:vender不对:" + item.__str__())
            return False
        if item[6] not in ["宏站", "室分", "室外"]:
            print("ENodeB:style不对:" + item.__str__())
            return False
        return True

    def insertAll(self):
        script = 'INSERT INTO enodeb VALUES(?,?,?,?,?,?,?)'
        self.cursor.executemany(script, self.itemList)
        self.cursor.commit()