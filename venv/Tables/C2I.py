class C2I:
    def __init__(self, cursor):
        self.filePath = 'C:\\Users\\dxxhj\\Desktop\\DBData\\10.tbC2I.csv'
        self.itemList = []
        self.cnt = 0
        self.cursor = cursor
        self.del_trig = """
        IF EXISTS (select * from sys.triggers where name='trig_c2i')
        BEGIN
            DROP TRIGGER trig_c2i
        END
        """
        self.trig = """
        CREATE trigger trig_c2i
        ON c2i
        INSTEAD OF INSERT
        AS
        BEGIN
        IF EXISTS(SELECT city FROM inserted)  --插入和更新
            DECLARE cur CURSOR forward_only 
            FOR SELECT * FROM inserted
            OPEN cur  --打开游标
            DECLARE @city NVARCHAR(255)
            DECLARE @scell_id NVARCHAR(255)
            DECLARE @ncell_id NVARCHAR(255)
            DECLARE @prc2i9 FLOAT
            DECLARE @c2i_mean FLOAT
            DECLARE @std FLOAT
            DECLARE @samplecount FLOAT
            DECLARE @weightedc2i FLOAT
            FETCH NEXT FROM cur INTO @city, @scell_id, @ncell_id, @prc2i9, @c2i_mean, @std, @samplecount, @weightedc2i
            WHILE(@@FETCH_STATUS=0) 
            BEGIN 
                --增加操作
        	    IF ((SELECT COUNT (*) FROM c2i WHERE scell_id=@scell_id AND ncell_id=@ncell_id)<1)
        	    BEGIN
        	        INSERT INTO c2i VALUES (@city, @scell_id, @ncell_id, @prc2i9, @c2i_mean, @std, @samplecount, @weightedc2i)
        	    END
        	    ELSE
        	    BEGIN
        	        UPDATE c2i SET 
        	        city=@city,
        	        prc2i9=@prc2i9, 
        	        c2i_mean=@c2i_mean, 
        	        std=@std, 
        	        samplecount=@samplecount, 
        	        weightedc2i=@weightedc2i
        	        WHERE scell_id=@scell_id AND ncell_id=@ncell_id
        	    END
                FETCH NEXT FROM cur INTO @city, @scell_id, @ncell_id, @prc2i9, @c2i_mean, @std, @samplecount, @weightedc2i   --指向下一条
            END 
            CLOSE cur  --关闭游标
            DEALLOCATE cur  --销毁游标资源
        END 
        """
        self.cursor.execute(self.del_trig)
        self.cursor.execute(self.trig)

    def loadData(self, size):
        city = ""
        sCellID = ""
        nCellID = ""
        prC2I9 = 0
        c2iMean = 0
        std = 0
        sampleCount = 0
        weightedC2I = 0
        with open(self.filePath) as file_obj:
            for content in file_obj:
                if self.cnt == size:
                    self.insertAll()
                    self.itemList.clear()
                    self.cnt = 0
                dataList = content.rstrip().split(",")
                city = dataList[0]
                sCellID = dataList[1]
                nCellID = dataList[2]
                prC2I9 = dataList[3]
                c2iMean = dataList[4]
                std = dataList[5]
                sampleCount = dataList[6]
                weightedC2I = dataList[7]
                if weightedC2I == '':
                    weightedC2I = float(prC2I9) * float(sampleCount)
                tempSqlItem = (city, sCellID, nCellID, prC2I9, c2iMean, std, sampleCount, weightedC2I)
                if self.isOK(tempSqlItem):
                    self.itemList.append(tempSqlItem)
                    self.cnt += 1
            if self.cnt != 0:
                self.insertAll()
                self.itemList.clear()

    def isOK(self, item):
        if float(item[7]) != float(item[3]) * float(item[6]):
            print("c2i:weightedC2I与prC2I9和sampleCount关系不对:" + item.__str__())
            return False
        return True

    def insertAll(self):
        script = 'INSERT INTO c2i VALUES(?,?,?,?,?,?,?,?)'
        self.cursor.executemany(script, self.itemList)
        self.cursor.commit()