class PCIAssignment:
    def __init__(self, cursor):
        self.filePath = 'C:\\Users\\dxxhj\\Desktop\\DBData\\5.tbPCIAssignment.csv'
        self.itemList = []
        self.cnt = 0
        self.cursor = cursor
        self.del_trig = """
        IF EXISTS (select * from sys.triggers where name='trig_pciassignment')
        BEGIN
            DROP TRIGGER trig_pciassignment
        END
        """
        self.trig = """
        CREATE trigger trig_pciassignment
        ON pciassignment
        INSTEAD OF INSERT
        AS
        BEGIN
        IF EXISTS(SELECT sector_id FROM inserted)  --插入和更新
            DECLARE cur CURSOR forward_only 
            FOR SELECT assign_id, earfcn, sector_id, sector_name, enodeb_id,
            pci, pss, sss, longitude, latitude, style, opt_datetime FROM inserted
            OPEN cur  --打开游标
            DECLARE @assign_id SMALLINT
            DECLARE @earfcn INT
            DECLARE @sector_id NVARCHAR(50)
            DECLARE @sector_name NVARCHAR(200)
            DECLARE @enodeb_id INT
            DECLARE @pci INT
            DECLARE @pss INT
            DECLARE @sss INT
            DECLARE @longitude FLOAT
            DECLARE @latitude FLOAT
            DECLARE @style VARCHAR(50)
            DECLARE @opt_datetime NVARCHAR(50)
            FETCH NEXT FROM cur INTO @assign_id, @earfcn, @sector_id, @sector_name, @enodeb_id,
            @pci, @pss, @sss, @longitude, @latitude, @style, @opt_datetime
            WHILE(@@FETCH_STATUS=0) 
            BEGIN 
                --增加操作
        	    IF ((SELECT COUNT (*) FROM pciassignment WHERE assign_id=@assign_id AND sector_id=@sector_id)<1)
        	    BEGIN
        	        INSERT INTO pciassignment VALUES (@assign_id, @earfcn, @sector_id, @sector_name, 
        	        @enodeb_id, @pci, @pss, @sss, @longitude, @latitude, @style, @opt_datetime)
        	    END
        	    ELSE
        	    BEGIN
        	        UPDATE pciassignment SET 
        	        earfcn=@earfcn, 
        	        sector_name=@sector_name, 
        	        enodeb_id=@enodeb_id,
        	        pci=@pci, 
        	        pss=@pss, 
        	        sss=@sss, 
        	        longitude=@longitude, 
        	        latitude=@latitude, 
        	        style=@style, 
        	        opt_datetime=@opt_datetime
        	        WHERE sector_id=@sector_id AND assign_id=@assign_id
        	    END
                FETCH NEXT FROM cur INTO @assign_id, @earfcn, @sector_id, @sector_name, @enodeb_id,
                @pci, @pss, @sss, @longitude, @latitude, @style, @opt_datetime   --指向下一条
            END 
            CLOSE cur  --关闭游标
            DEALLOCATE cur  --销毁游标资源
        END 
        """
        self.cursor.execute(self.del_trig)
        self.cursor.execute(self.trig)

    def loadData(self, size):
        assignID = 0
        earfcn = 0
        sectorID = ""
        sectorName = ""
        eNodeBID = 0
        pci = 0
        pss = 0
        sss = 0
        longitude = 0
        latitude = 0
        style = ""
        optDatetime = ""
        with open(self.filePath) as file_obj:
            for content in file_obj:
                if self.cnt == size:
                    self.insertAll()
                    self.itemList.clear()
                    self.cnt = 0
                dataList = content.rstrip().split(",")
                assignID = dataList[0]
                earfcn = dataList[1]
                sectorID = dataList[2]
                sectorName = dataList[3]
                eNodeBID = dataList[4]
                pci = dataList[5]
                pss = dataList[6]
                sss = dataList[7]
                longitude = dataList[8]
                latitude = dataList[9]
                style = dataList[10]
                optDatetime = dataList[11]
                if pss == '':
                    pss = int(pci) % 3
                if sss == '':
                    sss = int(int(pci) / 3)
                tempSqlItem = (assignID, earfcn, sectorID, sectorName,
                               eNodeBID, pci, pss, sss, longitude, latitude, style, optDatetime)
                if self.isOK(tempSqlItem):
                    self.itemList.append(tempSqlItem)
                    self.cnt += 1
            if self.cnt != 0:
                self.insertAll()
                self.itemList.clear()

    def isOK(self, item):
        if item[1] not in ['37900', '38098', '38400', '38496', '38544', '38950', '39148']:
            print("pciassignment:earfcn不对:" + item.__str__())
            return False
        if int(item[5]) > 503 or int(item[5]) < 0:
            print("Cell:pci不对:" + item.__str__())
            return False
        if item[6] not in ['0', '1', '2']:
            print("Cell:ecellType不对:" + item.__str__())
            return False
        if int(item[7]) > 167 or int(item[7]) < 0:
            print("Cell:ecellType不对:" + item.__str__())
            return False
        if item[10] not in ["宏站", "室分", "室外"]:
            print("pciassignment:ecellType不对:" + item.__str__())
            return False
        if int(item[5]) != int(item[6]) + int(item[7]) * 3:
            print("PCIAssignment:pci,pss,sss关系不对:" + item.__str__())
            return False
        return True

    def insertAll(self):
        script = 'INSERT INTO pciassignment VALUES(?,?,?,?,?,?,?,?,?,?,?,?)'
        self.cursor.executemany(script, self.itemList)
        self.cursor.commit()