class OptCell:
    def __init__(self, cursor):
        self.filePath = 'C:\\Users\\dxxhj\\Desktop\\DBData\\4.tbOptCell.csv'
        self.itemList = []
        self.cnt = 0
        self.cursor = cursor
        self.del_trig = """
        IF EXISTS (select * from sys.triggers where name='trig_optcell')
        BEGIN
            DROP TRIGGER trig_optcell
        END
        """
        self.trig = """
        CREATE trigger trig_optcell
        ON optcell
        INSTEAD OF INSERT
        AS
        BEGIN
        IF EXISTS(SELECT sector_id FROM inserted)  --插入和更新
            DECLARE cur CURSOR forward_only FOR SELECT sector_id, earfcn, cell_type FROM inserted
            OPEN cur  --打开游标
            DECLARE @sector_id NVARCHAR(50)
            DECLARE @earfcn INT
            DECLARE @cell_type NVARCHAR(50)
            FETCH NEXT FROM cur INTO @sector_id, @earfcn, @cell_type
            WHILE(@@FETCH_STATUS=0) 
            BEGIN 
                --增加操作
        	    IF ((SELECT COUNT (*) FROM optcell WHERE sector_id=@sector_id)<1)
        	    BEGIN
        	        INSERT INTO optcell VALUES (@sector_id, @earfcn, @cell_type)
        	    END
        	    ELSE
        	    BEGIN
        	        UPDATE optcell SET 
        	        earfcn=@earfcn, 
        	        cell_type=@cell_type
        	        WHERE sector_id=@sector_id
        	    END
                FETCH NEXT FROM cur INTO @sector_id, @earfcn, @cell_type   --指向下一条
            END 
            CLOSE cur  --关闭游标
            DEALLOCATE cur  --销毁游标资源
        END 
        """
        self.cursor.execute(self.del_trig)
        self.cursor.execute(self.trig)

    def loadData(self, size):
        sectorID = ""
        earfcn = 0
        cellType = ""
        with open(self.filePath) as file_obj:
            for content in file_obj:
                if self.cnt % size == 0 and self.cnt != 0:
                    self.insertAll()
                    self.itemList.clear()
                dataList = content.rstrip().split(",")
                sectorID = dataList[0]
                earfcn = dataList[1]
                cellType = dataList[2]
                tempSqlItem = (sectorID, earfcn, cellType)
                if self.isOK(tempSqlItem):
                    self.itemList.append(tempSqlItem)
                    self.cnt += 1
            if self.cnt % size != 0:
                self.insertAll()
                self.itemList.clear()

    def isOK(self, item):
        if item[1] not in ['37900', '38098', '38400', '38496', '38544', '38950', '39148']:
            print("OptCell:earfcn不对:" + item.__str__())
            return False
        if item[2] not in ["优化区", "保护带"]:
            print("OptCell:ecellType不对:" + item.__str__())
            return False
        return True

    def insertAll(self):
        script = 'INSERT INTO optcell VALUES(?,?,?)'
        self.cursor.executemany(script, self.itemList)
        self.cursor.commit()