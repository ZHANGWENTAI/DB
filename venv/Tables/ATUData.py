class ATUData:
    def __init__(self, cursor):
        self.filePath = 'C:\\Users\\dxxhj\\Desktop\\DBData\\6.tbATUData.csv'
        self.itemList = []
        self.cnt = 0
        self.cursor = cursor
        self.del_trig = """
        IF EXISTS (select * from sys.triggers where name='trig_atudata')
        BEGIN
            DROP TRIGGER trig_atudata
        END
        """
        self.trig = """
        CREATE trigger trig_atudata
        ON atudata
        INSTEAD OF INSERT
        AS
        BEGIN
        IF EXISTS(SELECT seq FROM inserted)  --插入和更新
            DECLARE cur CURSOR forward_only 
            FOR SELECT seq, file_name, time_stamp, longitude, latitude, 
            cell_id, tac, earfcn, pci, rsrp, rs_sinr, 
            ncell_id_1, ncell_earfcn_1, ncell_pci_1, ncell_rsrp_1, 
            ncell_id_2, ncell_earfcn_2, ncell_pci_2, ncell_rsrp_2, 
            ncell_id_3, ncell_earfcn_3, ncell_pci_3, ncell_rsrp_3, 
            ncell_id_4, ncell_earfcn_4, ncell_pci_4, ncell_rsrp_4, 
            ncell_id_5, ncell_earfcn_5, ncell_pci_5, ncell_rsrp_5, 
            ncell_id_6, ncell_earfcn_6, ncell_pci_6, ncell_rsrp_6 FROM inserted
            OPEN cur  --打开游标
            DECLARE @seq BIGINT
            DECLARE @file_name NVARCHAR(255)
            DECLARE @time_stamp VARCHAR(100)
            DECLARE @longitude FLOAT
            DECLARE @latitude FLOAT
            DECLARE @cell_id NVARCHAR(50)
            DECLARE @tac INT
            DECLARE @earfcn INT
            DECLARE @pci SMALLINT
            DECLARE @rsrp FLOAT
            DECLARE @rs_sinr FLOAT
            DECLARE @ncell_id_1 NVARCHAR(50)
            DECLARE @ncell_earfcn_1 INT
            DECLARE @ncell_pci_1 SMALLINT
            DECLARE @ncell_rsrp_1 FLOAT
            DECLARE @ncell_id_2 NVARCHAR(50)
            DECLARE @ncell_earfcn_2 INT
            DECLARE @ncell_pci_2 SMALLINT
            DECLARE @ncell_rsrp_2 FLOAT
            DECLARE @ncell_id_3 NVARCHAR(50)
            DECLARE @ncell_earfcn_3 INT
            DECLARE @ncell_pci_3 SMALLINT
            DECLARE @ncell_rsrp_3 FLOAT
            DECLARE @ncell_id_4 NVARCHAR(50)
            DECLARE @ncell_earfcn_4 INT
            DECLARE @ncell_pci_4 SMALLINT
            DECLARE @ncell_rsrp_4 FLOAT
            DECLARE @ncell_id_5 NVARCHAR(50)
            DECLARE @ncell_earfcn_5 INT
            DECLARE @ncell_pci_5 SMALLINT
            DECLARE @ncell_rsrp_5 FLOAT
            DECLARE @ncell_id_6 NVARCHAR(50)
            DECLARE @ncell_earfcn_6 INT
            DECLARE @ncell_pci_6 SMALLINT
            DECLARE @ncell_rsrp_6 FLOAT
            FETCH NEXT FROM cur INTO @seq, @file_name, @time_stamp, @longitude,
            @latitude, @cell_id, @tac, @earfcn, @pci, @rsrp, @rs_sinr, 
            @ncell_id_1, @ncell_earfcn_1, @ncell_pci_1, @ncell_rsrp_1, 
            @ncell_id_2, @ncell_earfcn_2, @ncell_pci_2, @ncell_rsrp_2, 
            @ncell_id_3, @ncell_earfcn_3, @ncell_pci_3, @ncell_rsrp_3, 
            @ncell_id_4, @ncell_earfcn_4, @ncell_pci_4, @ncell_rsrp_4, 
            @ncell_id_5, @ncell_earfcn_5, @ncell_pci_5, @ncell_rsrp_5, 
            @ncell_id_6, @ncell_earfcn_6, @ncell_pci_6, @ncell_rsrp_6
            WHILE(@@FETCH_STATUS=0) 
            BEGIN 
                --增加操作
                IF (@ncell_id_1='null') BEGIN SET @ncell_id_1=null END 
                IF (@ncell_id_2='null') BEGIN SET @ncell_id_2=null END 
                IF (@ncell_id_3='null') BEGIN SET @ncell_id_3=null END 
                IF (@ncell_id_4='null') BEGIN SET @ncell_id_4=null END 
                IF (@ncell_id_5='null') BEGIN SET @ncell_id_5=null END 
                IF (@ncell_id_6='null') BEGIN SET @ncell_id_6=null END 
                IF (@ncell_earfcn_1=-1) BEGIN SET @ncell_earfcn_1=null END 
                IF (@ncell_earfcn_2=-1) BEGIN SET @ncell_earfcn_2=null END 
                IF (@ncell_earfcn_3=-1) BEGIN SET @ncell_earfcn_3=null END 
                IF (@ncell_earfcn_4=-1) BEGIN SET @ncell_earfcn_4=null END 
                IF (@ncell_earfcn_5=-1) BEGIN SET @ncell_earfcn_5=null END 
                IF (@ncell_earfcn_6=-1) BEGIN SET @ncell_earfcn_6=null END 
                IF (@ncell_pci_1=-1) BEGIN SET @ncell_pci_1=null END 
                IF (@ncell_pci_2=-1) BEGIN SET @ncell_pci_2=null END 
                IF (@ncell_pci_3=-1) BEGIN SET @ncell_pci_3=null END 
                IF (@ncell_pci_4=-1) BEGIN SET @ncell_pci_4=null END 
                IF (@ncell_pci_5=-1) BEGIN SET @ncell_pci_5=null END 
                IF (@ncell_pci_6=-1) BEGIN SET @ncell_pci_6=null END 
                IF (@ncell_rsrp_1=-1) BEGIN SET @ncell_rsrp_1=null END 
                IF (@ncell_rsrp_2=-1) BEGIN SET @ncell_rsrp_2=null END 
                IF (@ncell_rsrp_3=-1) BEGIN SET @ncell_rsrp_3=null END 
                IF (@ncell_rsrp_4=-1) BEGIN SET @ncell_rsrp_4=null END 
                IF (@ncell_rsrp_5=-1) BEGIN SET @ncell_rsrp_5=null END 
                IF (@ncell_rsrp_6=-1) BEGIN SET @ncell_rsrp_6=null END 
        	    IF ((SELECT COUNT (*) FROM atudata WHERE seq=@seq AND file_name=@file_name)<1)
        	    BEGIN
        	        INSERT INTO atudata VALUES (@seq, @file_name, @time_stamp, @longitude,
                    @latitude, @cell_id, @tac, @earfcn, @pci, @rsrp, @rs_sinr, 
                    @ncell_id_1, @ncell_earfcn_1, @ncell_pci_1, @ncell_rsrp_1, 
                    @ncell_id_2, @ncell_earfcn_2, @ncell_pci_2, @ncell_rsrp_2, 
                    @ncell_id_3, @ncell_earfcn_3, @ncell_pci_3, @ncell_rsrp_3, 
                    @ncell_id_4, @ncell_earfcn_4, @ncell_pci_4, @ncell_rsrp_4, 
                    @ncell_id_5, @ncell_earfcn_5, @ncell_pci_5, @ncell_rsrp_5, 
                    @ncell_id_6, @ncell_earfcn_6, @ncell_pci_6, @ncell_rsrp_6)
        	    END
        	    ELSE
        	    BEGIN
        	        UPDATE atudata SET 
                    time_stamp=@time_stamp,
                    longitude=@longitude,
                    latitude=@latitude,
                    cell_id=@cell_id,
                    tac=@tac,
                    earfcn=@earfcn,
                    pci=@pci,
                    rsrp=@rsrp,
                    rs_sinr=@rs_sinr,
                    ncell_id_1=@ncell_id_1,
                    ncell_earfcn_1=@ncell_earfcn_1,
                    ncell_pci_1=@ncell_pci_1,
                    ncell_rsrp_1=@ncell_rsrp_1,
                    ncell_id_2=@ncell_id_2,
                    ncell_earfcn_2=@ncell_earfcn_2,
                    ncell_pci_2=@ncell_pci_2,
                    ncell_rsrp_2=@ncell_rsrp_2,
                    ncell_id_3=@ncell_id_3,
                    ncell_earfcn_3=@ncell_earfcn_3,
                    ncell_pci_3=@ncell_pci_3,
                    ncell_rsrp_3=@ncell_rsrp_3,
                    ncell_id_4=@ncell_id_4,
                    ncell_earfcn_4=@ncell_earfcn_4,
                    ncell_pci_4=@ncell_pci_4,
                    ncell_rsrp_4=@ncell_rsrp_4,
                    ncell_id_5=@ncell_id_5,
                    ncell_earfcn_5=@ncell_earfcn_5,
                    ncell_pci_5=@ncell_pci_5,
                    ncell_rsrp_5=@ncell_rsrp_5,
                    ncell_id_6=@ncell_id_6,
                    ncell_earfcn_6=@ncell_earfcn_6,
                    ncell_pci_6=@ncell_pci_6,
                    ncell_rsrp_6=@ncell_rsrp_6
        	        WHERE seq=@seq AND file_name=@file_name
        	    END
                FETCH NEXT FROM cur INTO @seq, @file_name, @time_stamp, @longitude,
                @latitude, @cell_id, @tac, @earfcn, @pci, @rsrp, @rs_sinr, 
                @ncell_id_1, @ncell_earfcn_1, @ncell_pci_1, @ncell_rsrp_1, 
                @ncell_id_2, @ncell_earfcn_2, @ncell_pci_2, @ncell_rsrp_2, 
                @ncell_id_3, @ncell_earfcn_3, @ncell_pci_3, @ncell_rsrp_3, 
                @ncell_id_4, @ncell_earfcn_4, @ncell_pci_4, @ncell_rsrp_4, 
                @ncell_id_5, @ncell_earfcn_5, @ncell_pci_5, @ncell_rsrp_5, 
                @ncell_id_6, @ncell_earfcn_6, @ncell_pci_6, @ncell_rsrp_6   --指向下一条
            END 
            CLOSE cur  --关闭游标
            DEALLOCATE cur  --销毁游标资源
        END 
        """
        self.cursor.execute(self.del_trig)
        self.cursor.execute(self.trig)

    def loadData(self, size):
        seq = 0
        fileName = ""
        timeStamp = ""
        longitude = 0
        latitude = 0
        cellID = ""
        tac = 0
        earfcn = 0
        pci = 0
        rsrp = 0
        rsSinr = 0
        nCellID1 = ""
        nCellEarfcn1 = 0
        nCellPCI1 = 0
        nCellRsrp1 = 0
        nCellID2 = ""
        nCellEarfcn2 = 0
        nCellPCI2 = 0
        nCellRsrp2 = 0
        nCellID3 = ""
        nCellEarfcn3 = 0
        nCellPCI3 = 0
        nCellRsrp3 = 0
        nCellID4 = ""
        nCellEarfcn4 = 0
        nCellPCI4 = 0
        nCellRsrp4 = 0
        nCellID5 = ""
        nCellEarfcn5 = 0
        nCellPCI5 = 0
        nCellRsrp5 = 0
        nCellID6 = ""
        nCellEarfcn6 = 0
        nCellPCI6 = 0
        nCellRsrp6 = 0
        with open(self.filePath) as file_obj:
            for content in file_obj:
                if self.cnt == size:
                    self.insertAll()
                    self.itemList.clear()
                    self.cnt = 0
                dataList = content.rstrip().split(",")
                seq = dataList[0]
                fileName = dataList[1]
                timeStamp = dataList[2]
                longitude = dataList[3]
                latitude = dataList[4]
                cellID = dataList[5]
                tac = dataList[6]
                earfcn = dataList[7]
                pci = dataList[8]
                rsrp = dataList[9]
                rsSinr = dataList[10]
                nCellID1 = self.getNullData(dataList[11], 'str')
                nCellEarfcn1 = self.getNullData(dataList[12], 'num')
                nCellPCI1 = self.getNullData(dataList[13], 'num')
                nCellRsrp1 = self.getNullData(dataList[14], 'num')
                nCellID2 = self.getNullData(dataList[15], 'str')
                nCellEarfcn2 = self.getNullData(dataList[16], 'num')
                nCellPCI2 = self.getNullData(dataList[17], 'num')
                nCellRsrp2 = self.getNullData(dataList[18], 'num')
                nCellID3 = self.getNullData(dataList[19], 'str')
                nCellEarfcn3 = self.getNullData(dataList[20], 'num')
                nCellPCI3 = self.getNullData(dataList[21], 'num')
                nCellRsrp3 = self.getNullData(dataList[22], 'num')
                nCellID4 = self.getNullData(dataList[23], 'str')
                nCellEarfcn4 = self.getNullData(dataList[24], 'num')
                nCellPCI4 = self.getNullData(dataList[25], 'num')
                nCellRsrp4 = self.getNullData(dataList[26], 'num')
                nCellID5 = self.getNullData(dataList[27], 'str')
                nCellEarfcn5 = self.getNullData(dataList[28], 'num')
                nCellPCI5 = self.getNullData(dataList[29], 'num')
                nCellRsrp5 = self.getNullData(dataList[30], 'num')
                nCellID6 = self.getNullData(dataList[31], 'str')
                nCellEarfcn6 = self.getNullData(dataList[32], 'num')
                nCellPCI6 = self.getNullData(dataList[33], 'num')
                nCellRsrp6 = self.getNullData(dataList[34], 'num')
                tempSqlItem = (seq, fileName, timeStamp, longitude, latitude,
                            cellID, tac, earfcn, pci, rsrp, rsSinr,
                            nCellID1, nCellEarfcn1, nCellPCI1, nCellRsrp1,
                            nCellID2, nCellEarfcn2, nCellPCI2, nCellRsrp2,
                            nCellID3, nCellEarfcn3, nCellPCI3, nCellRsrp3,
                            nCellID4, nCellEarfcn4, nCellPCI4, nCellRsrp4,
                            nCellID5, nCellEarfcn5, nCellPCI5, nCellRsrp5,
                            nCellID6, nCellEarfcn6, nCellPCI6, nCellRsrp6)
                if self.isOK(tempSqlItem):
                    self.itemList.append(tempSqlItem)
                    self.cnt += 1
            if self.cnt != 0:
                self.insertAll()
                self.itemList.clear()

    def getNullData(self, data, type):
        if data != '':
            return data
        if type == 'num':
            return -1
        return 'null'

    def isOK(self, item):
        if int(item[8]) > 503 or int(item[8]) < 0:
            print("ATUData:pci不对:" + item.__str__())
            return False
        if item[7] not in ['37900', '38098', '38400', '38496', '38544', '38950', '39148']:
            print("Cell:earfcn不对:" + item.__str__())
            return False
        return True

    def insertAll(self):
        script = 'INSERT INTO atudata VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        self.cursor.executemany(script, self.itemList)
        self.cursor.commit()