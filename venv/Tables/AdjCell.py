class AdjCell:
    def __init__(self, cursor):
        self.filePath = 'C:\\Users\\dxxhj\\Desktop\\DBData\\2.tbAdjCell.csv'
        self.itemList = []
        self.cnt = 0
        self.cursor = cursor
        self.del_trig = """
        IF EXISTS (select * from sys.triggers where name='trig_adjcell')
        BEGIN
            DROP TRIGGER trig_adjcell
        END
        """
        self.trig = """
        CREATE trigger trig_adjcell
        ON adjcell
        INSTEAD OF INSERT
        AS
        BEGIN
        IF EXISTS(SELECT s_sector_id FROM inserted)  --插入和更新
            DECLARE cur CURSOR forward_only FOR SELECT s_sector_id, n_sector_id, s_earfcn, n_earfcn FROM inserted
            OPEN cur  --打开游标
            DECLARE @s_sector_id VARCHAR(255)
            DECLARE @n_sector_id VARCHAR(255)
            DECLARE @s_earfcn INT
            DECLARE @n_earfcn INT
            FETCH NEXT FROM cur INTO @s_sector_id, @n_sector_id, @s_earfcn, @n_earfcn
            WHILE(@@FETCH_STATUS=0) 
            BEGIN 
                --增加操作
        	    IF ((SELECT COUNT (*) FROM adjcell WHERE s_sector_id=@s_sector_id AND n_sector_id=@n_sector_id)<1)
        	    BEGIN
        	        INSERT INTO adjcell VALUES (@s_sector_id, @n_sector_id, @s_earfcn, @n_earfcn)
        	    END
        	    ELSE
        	    BEGIN
        	        UPDATE adjcell SET 
        	        s_earfcn=@s_earfcn,
        	        n_earfcn=@n_earfcn
        	        WHERE s_sector_id=@s_sector_id AND n_sector_id=@n_sector_id
        	    END
                FETCH NEXT FROM cur INTO @s_sector_id, @n_sector_id, @s_earfcn, @n_earfcn   --指向下一条
            END 
            CLOSE cur  --关闭游标
            DEALLOCATE cur  --销毁游标资源
        END 
        """
        self.cursor.execute(self.del_trig)
        self.cursor.execute(self.trig)

    def loadData(self, size):
        sSectorID = ""
        nSectorID = ""
        sEarfcn = 0
        nEarfcn = 0
        with open(self.filePath) as file_obj:
            for content in file_obj:
                if self.cnt % size == 0 and self.cnt != 0:
                    self.insertAll()
                    self.itemList.clear()
                dataList = content.rstrip().split(",")
                sSectorID = dataList[0]
                nSectorID = dataList[1]
                sEarfcn = dataList[2]
                nEarfcn = dataList[3]
                tempSqlItem = (sSectorID, nSectorID, sEarfcn, nEarfcn)
                if self.isOK(tempSqlItem):
                    self.itemList.append(tempSqlItem)
                    self.cnt += 1
            if self.cnt % size != 0:
                self.insertAll()
                self.itemList.clear()

    def isOK(self, item):
        if item[2] not in ['37900', '38098', '38400', '38496', '38544', '38950', '39148']:
            print("AdjCell:s_earfcn不对:" + item.__str__())
            return False
        if item[3] not in ['37900', '38098', '38400', '38496', '38544', '38950', '39148']:
            print("AdjCell:n_earfcn不对:" + item.__str__())
            return False
        return True

    def insertAll(self):
        script = 'INSERT INTO adjcell VALUES(?,?,?,?)'
        self.cursor.executemany(script, self.itemList)
        self.cursor.commit()