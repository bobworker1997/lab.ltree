# Data Struct

```mermaid
graph TD
	 all --> Company1
	 all --> Company2
	 Company1 --> SubSystem1
	 Company1 --> SubSystem2

     SubSystem1 --> WebId1
     SubSystem1 --> WebId2
     SubSystem2 --> WebId3
     SubSystem2 --> WebId4
     
     WebId1 --> Player1
     WebId1 --> Player2
     WebId1 --> Player3

```

## Tree Data
- 10 Company
- 1 Company per 15 SubSystem, total 150
- 1 SubSystem per 20 WebId, total 3,000
- 1 WebId per 10 player, total 30,000

## Record Data
1 player per 20 records, total 600,000