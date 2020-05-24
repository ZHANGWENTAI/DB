from Tables.OptCell import OptCell
from Tables.ENodeB import ENodeB
from Tables.Cell import Cell
from Tables.AdjCell import AdjCell
from Tables.SecAdjCell import SecAdjCell
from Tables.PCIAssignment import PCIAssignment
from Tables.ATUData import ATUData
from Tables.ATUC2I import ATUC2I
from Tables.ATUHandOver import ATUHandOver
from Tables.MROData import MROData
from Tables.C2I import C2I
from Tables.HandOver import HandOver

class CreateTable:
    def __init__(self):
        self.create = """
USE 课设   
        
IF EXISTS(SELECT * FROM sysobjects WHERE NAME='chk_enodeb_vendor')
	ALTER TABLE enodeb DROP CONSTRAINT chk_enodeb_vendor;
IF EXISTS(SELECT * FROM sysobjects WHERE NAME='chk_enodeb_style')
	ALTER TABLE enodeb DROP CONSTRAINT chk_enodeb_style;
IF EXISTS(SELECT * FROM sysobjects WHERE NAME='chk_cell_pci')
	ALTER TABLE cell DROP CONSTRAINT chk_cell_pci;
IF EXISTS(SELECT * FROM sysobjects WHERE NAME='chk_cell_earfcn')
	ALTER TABLE cell DROP CONSTRAINT chk_cell_earfcn;
IF EXISTS(SELECT * FROM sysobjects WHERE NAME='chk_cell_pss')
	ALTER TABLE cell DROP CONSTRAINT chk_cell_pss;
IF EXISTS(SELECT * FROM sysobjects WHERE NAME='chk_cell_sss')
	ALTER TABLE cell DROP CONSTRAINT chk_cell_sss;
IF EXISTS(SELECT * FROM sysobjects WHERE NAME='chk_cell_totletilt')
	ALTER TABLE cell DROP CONSTRAINT chk_cell_totletilt;
IF EXISTS(SELECT * FROM sysobjects WHERE NAME='chk_optcell_earfcn')
	ALTER TABLE optcell DROP CONSTRAINT chk_optcell_earfcn;
IF EXISTS(SELECT * FROM sysobjects WHERE NAME='chk_optcell_celltype')
	ALTER TABLE optcell DROP CONSTRAINT chk_optcell_celltype;
IF EXISTS(SELECT * FROM sysobjects WHERE NAME='chk_pciassignment_earfcn')
	ALTER TABLE pciassignment DROP CONSTRAINT chk_pciassignment_earfcn;
IF EXISTS(SELECT * FROM sysobjects WHERE NAME='chk_pciassignment_style')
	ALTER TABLE pciassignment DROP CONSTRAINT chk_pciassignment_style;
IF EXISTS(SELECT * FROM sysobjects WHERE NAME='fk_cell_enodeb_1')
	ALTER TABLE cell DROP CONSTRAINT fk_cell_enodeb_1;

IF EXISTS(SELECT TOP 1 * FROM sysObjects WHERE Id=OBJECT_ID(N'enodeb') and xtype='U')
	DROP TABLE enodeb;
IF EXISTS(SELECT TOP 1 * FROM sysObjects WHERE Id=OBJECT_ID(N'cell') and xtype='U')
	DROP TABLE cell;
IF EXISTS(SELECT TOP 1 * FROM sysObjects WHERE Id=OBJECT_ID(N'adjcell') and xtype='U')
	DROP TABLE adjcell;
IF EXISTS(SELECT TOP 1 * FROM sysObjects WHERE Id=OBJECT_ID(N'secadjcell') and xtype='U')
	DROP TABLE secadjcell;
IF EXISTS(SELECT TOP 1 * FROM sysObjects WHERE Id=OBJECT_ID(N'optcell') and xtype='U')
	DROP TABLE optcell;
IF EXISTS(SELECT TOP 1 * FROM sysObjects WHERE Id=OBJECT_ID(N'pciassignment') and xtype='U')
	DROP TABLE pciassignment;
IF EXISTS(SELECT TOP 1 * FROM sysObjects WHERE Id=OBJECT_ID(N'atudata') and xtype='U')
	DROP TABLE atudata;
IF EXISTS(SELECT TOP 1 * FROM sysObjects WHERE Id=OBJECT_ID(N'atuc2i') and xtype='U')
	DROP TABLE atuc2i;
IF EXISTS(SELECT TOP 1 * FROM sysObjects WHERE Id=OBJECT_ID(N'atuhandover') and xtype='U')
	DROP TABLE atuhandover;
IF EXISTS(SELECT TOP 1 * FROM sysObjects WHERE Id=OBJECT_ID(N'mrodata') and xtype='U')
	DROP TABLE mrodata;
IF EXISTS(SELECT TOP 1 * FROM sysObjects WHERE Id=OBJECT_ID(N'c2i') and xtype='U')
	DROP TABLE c2i;
IF EXISTS(SELECT TOP 1 * FROM sysObjects WHERE Id=OBJECT_ID(N'handover') and xtype='U')
	DROP TABLE handover;

CREATE TABLE enodeb
(
	city NVARCHAR(255) NULL,
	enodebid INT PRIMARY KEY,
	enodeb_name NVARCHAR(255) NOT NULL,
	vendor NVARCHAR(255),
	longitude FLOAT NOT NULL,
	latitude FLOAT NOT NULL,
	style NVARCHAR(255),
	CONSTRAINT chk_enodeb_vendor CHECK(vendor='华为' OR vendor='中兴' OR vendor='诺西' OR vendor='爱立信' OR vendor='贝尔' OR vendor='大唐'),
	CONSTRAINT chk_enodeb_style CHECK(style='宏站' OR style='室分' OR style='室外')
)

CREATE TABLE cell
(
	city NVARCHAR(255) NULL,
	sector_id NVARCHAR(255) PRIMARY KEY,
	sector_name NVARCHAR(255) NOT NULL,
	enodebid INT,
	enode_name NVARCHAR(255) NOT NULL,
	earfcn INT NOT NULL,
	pci INT NOT NULL,
	pss INT NULL,
	sss INT NULL,
	tac INT,
	azimuth FLOAT NOT NULL,
	height FLOAT,
	electtilt FLOAT,
	mechtilt FLOAT,
	totletilt FLOAT NOT NULL,
	CONSTRAINT fk_cell_enodeb_1 FOREIGN KEY (enodebid) REFERENCES enodeb(enodebid),
	CONSTRAINT chk_cell_pci CHECK(pci BETWEEN 0 AND 503),
	CONSTRAINT chk_cell_earfcn CHECK(earfcn=37900 OR earfcn=38098 OR earfcn=38400 OR earfcn=38544 OR earfcn=38496 OR earfcn=38950 OR earfcn=39148),
	CONSTRAINT chk_cell_pss CHECK(pss=0 OR pss=1 OR pss=2 OR pss=NULL),
	CONSTRAINT chk_cell_sss CHECK(sss=NULL OR sss BETWEEN 0 AND 167),
	CONSTRAINT chk_cell_totletilt CHECK(totletilt=electtilt+mechtilt),
)

CREATE TABLE adjcell
(
	s_sector_id VARCHAR(255),
	n_sector_id VARCHAR(255),
	s_earfcn INT,
	n_earfcn INT,
	PRIMARY KEY(s_sector_id, n_sector_id)
)

CREATE TABLE secadjcell
(
	s_sector_id VARCHAR(255),
	n_sector_id VARCHAR(255),
	PRIMARY KEY(s_sector_id, n_sector_id)
)

CREATE TABLE optcell
(
	sector_id NVARCHAR(50) PRIMARY KEY,
	earfcn INT,
	cell_type NVARCHAR(50),
	CONSTRAINT chk_optcell_earfcn CHECK(earfcn=37900 OR earfcn=38098 OR earfcn=38400 OR earfcn=38544 OR earfcn=38496 OR earfcn=38950 OR earfcn=39148),
	CONSTRAINT chk_optcell_celltype CHECK(cell_type='优化区' OR cell_type='保护带'),
)

CREATE TABLE pciassignment
(
	assign_id SMALLINT,
	earfcn INT,
	sector_id NVARCHAR(50),
	sector_name NVARCHAR(200),
	enodeb_id INT,
	pci INT,
	pss INT,
	sss INT,
	longitude FLOAT,
	latitude FLOAT,
	style VARCHAR(50),
	opt_datetime NVARCHAR(50),
	PRIMARY KEY(assign_id, sector_id),
	CONSTRAINT chk_pciassignment_earfcn CHECK(earfcn=37900 OR earfcn=38098 OR earfcn=38400 OR earfcn=38544 OR earfcn=38496 OR earfcn=38950 OR earfcn=39148),
	CONSTRAINT chk_pciassignment_style CHECK(style='宏站' OR style='室分' OR style='室外'),
)

CREATE TABLE atudata
(
	seq BIGINT,
	file_name NVARCHAR(255),
	time_stamp VARCHAR(100),
	longitude FLOAT,
	latitude FLOAT,
	cell_id NVARCHAR(50),
	tac INT,
	earfcn INT,
	pci SMALLINT,
	rsrp FLOAT,
	rs_sinr FLOAT,
	ncell_id_1 NVARCHAR(50),
	ncell_earfcn_1 INT,
	ncell_pci_1 SMALLINT,
	ncell_rsrp_1 FLOAT,
	ncell_id_2 NVARCHAR(50),
	ncell_earfcn_2 INT,
	ncell_pci_2 SMALLINT,
	ncell_rsrp_2 FLOAT,
	ncell_id_3 NVARCHAR(50),
	ncell_earfcn_3 INT,
	ncell_pci_3 SMALLINT,
	ncell_rsrp_3 FLOAT,
	ncell_id_4 NVARCHAR(50),
	ncell_earfcn_4 INT,
	ncell_pci_4 SMALLINT,
	ncell_rsrp_4 FLOAT,
	ncell_id_5 NVARCHAR(50),
	ncell_earfcn_5 INT,
	ncell_pci_5 SMALLINT,
	ncell_rsrp_5 FLOAT,
	ncell_id_6 NVARCHAR(50),
	ncell_earfcn_6 INT,
	ncell_pci_6 SMALLINT,
	ncell_rsrp_6 FLOAT,
	PRIMARY KEY(seq, file_name)
)

CREATE TABLE atuc2i
(
	sector_id NVARCHAR(50),
	ncell_id NVARCHAR(50),
	ratio_all FLOAT,
	rank INT,
	cosite TINYINT,
	PRIMARY KEY(ncell_id, sector_id)
)

CREATE TABLE atuhandover
(
	s_sector_id NVARCHAR(50),
	n_sector_id NVARCHAR(50),
	hoatt INT,
	PRIMARY KEY(s_sector_id, n_sector_id)
)

CREATE TABLE mrodata
(
	time_stamp NVARCHAR(30),
	serving_sector NVARCHAR(50),
	interfering_sector NVARCHAR(50),
	lte_sc_rsrp FLOAT,
	lte_nc_rsrp FLOAT,
	lte_nc_earfcn INT,
	lte_nc_pci SMALLINT,
	PRIMARY KEY(time_stamp, serving_sector, interfering_sector)
)

CREATE TABLE c2i
(
	city NVARCHAR(255),
	scell_id NVARCHAR(255),
	ncell_id NVARCHAR(255),
	prc2i9 FLOAT,
	c2i_mean FLOAT,
	std FLOAT,
	samplecount FLOAT,
	weightedc2i FLOAT,
	PRIMARY KEY(scell_id, ncell_id)
)

CREATE TABLE handover
(
	city NVARCHAR(255),
	scell_id VARCHAR(50),
	ncell_id VARCHAR(50),
	hoatt INT,
	hosucc INT,
	hosuccrate FLOAT,
	PRIMARY KEY(scell_id, ncell_id)
)
"""
    def loadData(self, cursor):
        enodeb = ENodeB(cursor)
        enodeb.loadData(50)
        print("enodeb finished!")

        cell = Cell(cursor)
        cell.loadData(50)
        print("cell finished!")

        adjcell = AdjCell(cursor)
        adjcell.loadData(1000)
        print("adjcell finished!")

        secadjcell = SecAdjCell(cursor)
        secadjcell.loadData(2000)
        print("secadjcell finished!")

        optcell = OptCell(cursor)
        optcell.loadData(50)
        print("optcell finished!")

        pciassignment = PCIAssignment(cursor)
        pciassignment.loadData(1)
        print("pciassignment finished!")

        atudata = ATUData(cursor)
        atudata.loadData(1)
        print("atudata finished!")

        atuc2i = ATUC2I(cursor)
        atuc2i.loadData(50)
        print("atuc2i finished!")

        atuhandover = ATUHandOver(cursor)
        atuhandover.loadData(50)
        print("atuhandover finished!")

        mrodata = MROData(cursor)
        mrodata.loadData(2000)
        print("mrodata finished!")

        c2i = C2I(cursor)
        c2i.loadData(50)
        print("c2i finished!")

        handover = HandOver(cursor)
        handover.loadData(50)
        print("handover finished!")

    def createTable(self, cursor):    # c是数据库句柄
        cursor.execute(self.create)
        cursor.commit()
