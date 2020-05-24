class ATUC2I:
    def __init__(self, cursor):
        self.filePath = 'C:\\Users\\dxxhj\\Desktop\\DBData\\7.tbATUC2I.csv'
        self.itemList = []
        self.cnt = 0
        self.cursor = cursor
        self.del_trig = """
        IF EXISTS (select * from sys.triggers where name='trig_atuc2i')
        BEGIN
            DROP TRIGGER trig_atuc2i
        END
        """
        self.trig = """
        CREATE trigger trig_atuc2i
        ON atuc2i
        INSTEAD OF INSERT
        AS
        BEGIN
        IF EXISTS(SELECT sector_id FROM inserted)  --插入和更新
            DECLARE cur CURSOR forward_only 
            FOR SELECT * FROM inserted
            OPEN cur  --打开游标
            DECLARE @sector_id NVARCHAR(50)
            DECLARE @ncell_id NVARCHAR(50)
            DECLARE @ratio_all FLOAT
            DECLARE @rank INT
            DECLARE @cosite TINYINT
            FETCH NEXT FROM cur INTO @sector_id, @ncell_id, @ratio_all, @rank, @cosite
            WHILE(@@FETCH_STATUS=0) 
            BEGIN 
                --增加操作
        	    IF ((SELECT COUNT (*) FROM atuc2i WHERE sector_id=@sector_id AND ncell_id=@ncell_id)<1)
        	    BEGIN
        	        INSERT INTO atuc2i VALUES (@sector_id, @ncell_id, @ratio_all, @rank, @cosite)
        	    END
        	    ELSE
        	    BEGIN
        	        UPDATE atuc2i SET 
        	        ratio_all=@ratio_all,
                    rank=@rank, 
                    cosite=@cosite
        	        WHERE sector_id=@sector_id AND ncell_id=@ncell_id
        	    END
                FETCH NEXT FROM cur INTO @sector_id, @ncell_id, @ratio_all, @rank, @cosite   --指向下一条
            END 
            CLOSE cur  --关闭游标
            DEALLOCATE cur  --销毁游标资源
        END 
        """
        self.cursor.execute(self.del_trig)
        self.cursor.execute(self.trig)

    def loadData(self, size):
        sectorID = ""
        nCellID = ""
        ratioAll = 0
        rank = 0
        cosite = 0
        with open(self.filePath) as file_obj:
            for content in file_obj:
                if self.cnt == size:
                    self.insertAll()
                    self.itemList.clear()
                    self.cnt = 0
                dataList = content.rstrip().split(",")
                sectorID = dataList[0]
                nCellID = dataList[1]
                ratioAll = dataList[2]
                rank = dataList[3]
                cosite = dataList[4]
                tempSqlItem = (sectorID, nCellID, ratioAll, rank, cosite)
                if self.isOK(tempSqlItem):
                    self.itemList.append(tempSqlItem)
                    self.cnt += 1
            if self.cnt != 0:
                self.insertAll()
                self.itemList.clear()

    def isOK(self, item):
        if item[4] not in ["0", "1"]:
            print("atuc2i:cosite不对:" + item.__str__())
            return False
        return True

    def insertAll(self):
        script = 'INSERT INTO atuc2i VALUES(?,?,?,?,?)'
        self.cursor.executemany(script, self.itemList)
        self.cursor.commit()