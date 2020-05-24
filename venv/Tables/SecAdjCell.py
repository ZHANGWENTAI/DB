class SecAdjCell:
    def __init__(self, cursor):
        self.filePath = 'C:\\Users\\dxxhj\\Desktop\\DBData\\3.tbSecAdjcell.csv'
        self.itemList = []
        self.cnt = 0
        self.cursor = cursor
        self.del_trig = """
        IF EXISTS (select * from sys.triggers where name='trig_secadjcell')
        BEGIN
            DROP TRIGGER trig_secadjcell
        END
        """
        self.trig = """
        CREATE trigger trig_secadjcell
        ON secadjcell
        INSTEAD OF INSERT
        AS
        BEGIN
        IF EXISTS(SELECT s_sector_id FROM inserted)  --插入和更新
            DECLARE cur CURSOR forward_only FOR SELECT s_sector_id, n_sector_id FROM inserted
            OPEN cur  --打开游标
            DECLARE @s_sector_id VARCHAR(255)
            DECLARE @n_sector_id VARCHAR(255)
            FETCH NEXT FROM cur INTO @s_sector_id, @n_sector_id
            WHILE(@@FETCH_STATUS=0) 
            BEGIN 
                --增加操作
        	    IF ((SELECT COUNT (*) FROM secadjcell WHERE s_sector_id=@s_sector_id AND n_sector_id=@n_sector_id)<1)
        	    BEGIN
        	        INSERT INTO secadjcell VALUES (@s_sector_id, @n_sector_id)
        	    END
                FETCH NEXT FROM cur INTO @s_sector_id, @n_sector_id   --指向下一条
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
        with open(self.filePath) as file_obj:
            for content in file_obj:
                if self.cnt % size == 0 and self.cnt != 0:
                    self.insertAll()
                    self.itemList.clear()
                dataList = content.rstrip().split(",")
                sSectorID = dataList[0]
                nSectorID = dataList[1]
                tempSqlItem = (sSectorID, nSectorID)
                if self.isOK(tempSqlItem):
                    self.itemList.append(tempSqlItem)
                    self.cnt += 1
            if self.cnt % size != 0:
                self.insertAll()
                self.itemList.clear()

    def isOK(self, item):
        if item[0] == '':
            return False
        return True

    def insertAll(self):
        script = 'INSERT INTO secadjcell VALUES(?,?)'
        self.cursor.executemany(script, self.itemList)
        self.cursor.commit()