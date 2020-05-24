class MROData:
    def __init__(self, cursor):
        self.filePath = 'C:\\Users\\dxxhj\\Desktop\\DBData\\9.tbMROData.csv'
        self.itemList = []
        self.cnt = 0
        self.cursor = cursor
        self.del_trig = """
        IF EXISTS (select * from sys.triggers where name='trig_mrodata')
        BEGIN
            DROP TRIGGER trig_mrodata
        END
        """
        self.trig = """
        CREATE trigger trig_mrodata
        ON mrodata
        INSTEAD OF INSERT
        AS
        BEGIN
        IF EXISTS(SELECT time_stamp FROM inserted)  --插入和更新
            DECLARE cur CURSOR forward_only 
            FOR SELECT * FROM inserted
            OPEN cur  --打开游标
            DECLARE @time_stamp NVARCHAR(30)
            DECLARE @serving_sector NVARCHAR(50)
            DECLARE @interfering_sector NVARCHAR(50)
            DECLARE @lte_sc_rsrp FLOAT
            DECLARE @lte_nc_rsrp FLOAT
            DECLARE @lte_nc_earfcn INT
            DECLARE @lte_nc_pci SMALLINT
            FETCH NEXT FROM cur INTO @time_stamp, @serving_sector, @interfering_sector, 
            @lte_sc_rsrp, @lte_nc_rsrp, @lte_nc_earfcn, @lte_nc_pci
            WHILE(@@FETCH_STATUS=0) 
            BEGIN 
                --增加操作
        	    IF ((SELECT COUNT (*) FROM mrodata WHERE time_stamp=@time_stamp AND 
        	    serving_sector=@serving_sector AND interfering_sector=@interfering_sector)<1)
        	    BEGIN
        	        INSERT INTO mrodata VALUES (@time_stamp, @serving_sector, @interfering_sector, 
                    @lte_sc_rsrp, @lte_nc_rsrp, @lte_nc_earfcn, @lte_nc_pci)
        	    END
        	    ELSE
        	    BEGIN
        	        UPDATE mrodata SET 
                    lte_sc_rsrp=@lte_sc_rsrp, 
                    lte_nc_rsrp=@lte_nc_rsrp, 
                    lte_nc_earfcn=@lte_nc_earfcn, 
                    lte_nc_pci=@lte_nc_pci
        	        WHERE time_stamp=@time_stamp AND serving_sector=@serving_sector AND 
        	        interfering_sector=@interfering_sector
        	    END
                FETCH NEXT FROM cur INTO @time_stamp, @serving_sector, @interfering_sector, 
                @lte_sc_rsrp, @lte_nc_rsrp, @lte_nc_earfcn, @lte_nc_pci   --指向下一条
            END 
            CLOSE cur  --关闭游标
            DEALLOCATE cur  --销毁游标资源
        END 
        """
        self.cursor.execute(self.del_trig)
        self.cursor.execute(self.trig)

    def loadData(self, size):
        timeStamp = ""
        servingSector = ""
        interferingSector = ""
        lteSCRsrp = 0
        lteNCRsrp = 0
        lteNCEarfcn = 0
        lteNCPCI = 0
        with open(self.filePath) as file_obj:
            for content in file_obj:
                if self.cnt == size:
                    self.insertAll()
                    self.itemList.clear()
                    self.cnt = 0
                dataList = content.rstrip().split(",")
                timeStamp = dataList[0]
                servingSector = dataList[1]
                interferingSector = dataList[2]
                lteSCRsrp = dataList[3]
                lteNCRsrp = dataList[4]
                lteNCEarfcn = dataList[5]
                lteNCPCI = dataList[6]
                tempSqlItem = (timeStamp, servingSector, interferingSector, lteSCRsrp, lteNCRsrp, lteNCEarfcn, lteNCPCI)
                if self.isOK(tempSqlItem):
                    self.itemList.append(tempSqlItem)
                    self.cnt += 1
            if self.cnt != 0:
                self.insertAll()
                self.itemList.clear()

    def isOK(self, item):
        if item[5] not in ['37900', '38098', '38400', '38496', '38544', '38950', '39148']:
            print("MROData:earfcn不对:" + item.__str__())
            return False
        if int(item[6]) > 503 or int(item[6]) < 0:
            print("MROData:pci不对:" + item.__str__())
            return False
        return True

    def insertAll(self):
        script = 'INSERT INTO mrodata VALUES(?,?,?,?,?,?,?)'
        self.cursor.executemany(script, self.itemList)
        self.cursor.commit()