class ATUHandOver:
    def __init__(self, cursor):
        self.filePath = 'C:\\Users\\dxxhj\\Desktop\\DBData\\8.tbATUHandOver.csv'
        self.itemList = []
        self.cnt = 0
        self.cursor = cursor
        self.del_trig = """
        IF EXISTS (select * from sys.triggers where name='trig_atuhandover')
        BEGIN
            DROP TRIGGER trig_atuhandover
        END
        """
        self.trig = """
        CREATE trigger trig_atuhandover
        ON atuhandover
        INSTEAD OF INSERT
        AS
        BEGIN
        IF EXISTS(SELECT s_sector_id FROM inserted)  --插入和更新
            DECLARE cur CURSOR forward_only 
            FOR SELECT * FROM inserted
            OPEN cur  --打开游标
            DECLARE @s_sector_id NVARCHAR(50)
            DECLARE @n_sector_id NVARCHAR(50)
            DECLARE @hoatt INT
            FETCH NEXT FROM cur INTO @s_sector_id, @n_sector_id, @hoatt
            WHILE(@@FETCH_STATUS=0) 
            BEGIN 
                --增加操作
        	    IF ((SELECT COUNT (*) FROM atuhandover WHERE s_sector_id=@s_sector_id AND n_sector_id=@n_sector_id)<1)
        	    BEGIN
        	        INSERT INTO atuhandover VALUES (@s_sector_id, @n_sector_id, @hoatt)
        	    END
        	    ELSE
        	    BEGIN
        	        UPDATE atuhandover SET 
        	        hoatt=@hoatt
        	        WHERE s_sector_id=@s_sector_id AND n_sector_id=@n_sector_id
        	    END
                FETCH NEXT FROM cur INTO @s_sector_id, @n_sector_id, @hoatt   --指向下一条
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
        hoatt = 0
        with open(self.filePath) as file_obj:
            for content in file_obj:
                if self.cnt == size:
                    self.insertAll()
                    self.itemList.clear()
                    self.cnt = 0
                dataList = content.rstrip().split(",")
                sSectorID = dataList[0]
                nSectorID = dataList[1]
                hoatt = dataList[2]
                tempSqlItem = (sSectorID, nSectorID, hoatt)
                self.itemList.append(tempSqlItem)
                self.cnt += 1
            if self.cnt != 0:
                self.insertAll()
                self.itemList.clear()

    def insertAll(self):
        script = 'INSERT INTO atuhandover VALUES(?,?,?)'
        self.cursor.executemany(script, self.itemList)
        self.cursor.commit()