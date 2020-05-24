class Cell:
    def __init__(self, cursor):
        self.filePath = 'C:\\Users\\dxxhj\\Desktop\\DBData\\1.tbCell.csv'
        self.itemList = []
        self.cnt = 0
        self.cursor = cursor
        self.del_trig = """
        IF EXISTS (select * from sys.triggers where name='trig_cell')
        BEGIN
        DROP TRIGGER trig_cell
        END
        """
        self.trig = """
        CREATE trigger trig_cell
        ON cell
        INSTEAD OF INSERT
        AS
        BEGIN
        IF EXISTS(SELECT sector_id FROM inserted)  --插入和更新
            DECLARE cur CURSOR forward_only 
            FOR SELECT city, sector_id, sector_name, enodebid, enode_name,
            earfcn, pci, pss, sss, tac, azimuth, height, 
            electtilt, mechtilt, totletilt FROM inserted
            OPEN cur  --打开游标
            DECLARE @city NVARCHAR(255)
            DECLARE @sector_id NVARCHAR(255)
            DECLARE @sector_name NVARCHAR(255)
            DECLARE @enodebid INT
            DECLARE @enode_name NVARCHAR(255)
            DECLARE @earfcn INT
            DECLARE @pci INT
            DECLARE @pss INT
            DECLARE @sss INT
            DECLARE @tac INT
            DECLARE @azimuth FLOAT
            DECLARE @height FLOAT
            DECLARE @electtilt FLOAT
            DECLARE @mechtilt FLOAT
            DECLARE @totletilt FLOAT
            FETCH NEXT FROM cur INTO @city, @sector_id, @sector_name, @enodebid, @enode_name,
            @earfcn, @pci, @pss, @sss, @tac, @azimuth, @height, @electtilt, @mechtilt, @totletilt
            WHILE(@@FETCH_STATUS=0) 
            BEGIN 
                --增加操作
        	    IF ((SELECT COUNT (*) FROM cell WHERE sector_id=@sector_id)<1)
        	    BEGIN
        	        IF ((SELECT COUNT (*) FROM enodeb WHERE enodebid=@enodebid)>=1)
        	        BEGIN
                        INSERT INTO cell VALUES (@city, @sector_id, @sector_name, 
                        @enodebid, @enode_name,@earfcn, @pci, @pss, @sss, @tac, 
                        @azimuth, @height, @electtilt, @mechtilt, @totletilt)
                    END 
        	    END
        	    ELSE
        	    BEGIN
        	        UPDATE cell SET 
        	        city=@city, 
        	        sector_name=@sector_name, 
        	        enodebid=@enodebid, 
        	        enode_name=@enode_name,
        	        earfcn=@earfcn, 
        	        pci=@pci, 
        	        pss=@pss, 
        	        sss=@sss, 
        	        tac=@tac, 
        	        azimuth=@azimuth, 
        	        height=@height, 
        	        electtilt=@electtilt, 
        	        mechtilt=@mechtilt, 
        	        totletilt=@totletilt
        	        WHERE sector_id=@sector_id
        	    END
                FETCH NEXT FROM cur INTO @city, @sector_id, @sector_name, @enodebid, @enode_name,
                @earfcn, @pci, @pss, @sss, @tac, @azimuth, @height, 
                @electtilt, @mechtilt, @totletilt   --指向下一条
            END 
            CLOSE cur  --关闭游标
            DEALLOCATE cur  --销毁游标资源
        END 
        """
        self.cursor.execute(self.del_trig)
        self.cursor.execute(self.trig)

    def loadData(self, size):
        city = ""
        sectorID = ""
        sectorName = ""
        eNodeBID = 0
        eNodeName = ""
        earfcn = 0
        pci = 0
        pss = 0
        sss = 0
        tac = 0
        azimuth = 0
        height = 0
        electtilt = 0
        mechtilt = 0
        totletilt = 0
        with open(self.filePath) as file_obj:
            for content in file_obj:
                if self.cnt == size - 1:
                    self.insertAll()
                    self.itemList.clear()
                    self.cnt = 0
                dataList = content.rstrip().split(",")
                city = dataList[0]
                sectorID = dataList[1]
                sectorName = dataList[2]
                eNodeBID = dataList[3]
                eNodeName = dataList[4]
                earfcn = dataList[5]
                pci = dataList[6]
                pss = dataList[7]
                sss = dataList[8]
                tac = dataList[9]
                azimuth = dataList[14]
                height = dataList[15]
                electtilt = dataList[16]
                mechtilt = dataList[17]
                totletilt = dataList[18]
                if pss == '':
                    sss = int(pci) % 3
                if sss == '':
                    sss = int(int(pci) / 3)
                if height == '':
                    height = 7
                if electtilt == '':
                    electtilt = 3
                if mechtilt == '':
                    mechtilt = 4
                if totletilt == '':
                    totletilt = 7
                tempSqlItem = (city, sectorID, sectorName, eNodeBID, eNodeName,
                    earfcn, pci, pss, sss, tac, azimuth, height,
                    electtilt, mechtilt, totletilt)
                if self.isOK(tempSqlItem):
                    self.itemList.append(tempSqlItem)
                    self.cnt += 1
            if self.cnt != 0:
                self.insertAll()
                self.itemList.clear()

    def isOK(self, item):
        if item[5] not in ['37900', '38098', '38400', '38496', '38544', '38950', '39148']:
            print("Cell:earfcn不对:" + item.__str__())
            return False
        if int(item[6]) > 503 or int(item[6]) < 0:
            print("Cell:pci不对:" + item.__str__())
            return False
        if item[7] not in ['0', '1', '2']:
            print("Cell:ecellType不对:" + item.__str__())
            return False
        if int(item[8]) > 167 or int(item[8]) < 0:
            print("Cell:ecellType不对:" + item.__str__())
            return False
        if str(item[11]).isnumeric() == False:
            return False
        if int(item[14]) != int(item[12]) + int(item[13]):
            return False
        if int(item[6]) != int(item[7]) + int(item[8]) * 3:
            print("Cell:pci,pss,sss关系不对:" + item.__str__())
            return False
        return True

    def insertAll(self):
        script = 'INSERT INTO cell VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        self.cursor.executemany(script, self.itemList)
        self.cursor.commit()