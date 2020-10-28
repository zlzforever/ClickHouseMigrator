##  --sheets
## 出库单
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/2020出库单M10.xlsx --database harmay --table orders --drop true
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/2020出库单Q3.xlsx --database harmay --table orders

## 出库单明细
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/2020出库明细Q3.xlsx --database harmay --table order_items --drop true
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/2020出库明细M10.xlsx --database harmay --table order_items

## 慧策
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/2020慧策销售订单M9.xlsx --database harmay --table huice_orders --drop true
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/2020慧策销售订单M10.xlsx --database harmay --table huice_orders

## 退货单
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/2020退货入库单M10.xlsx --database harmay --table refund_orders --drop true
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/2020退货入库单Q3.xlsx --database harmay --table refund_orders

## 退货明细
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/2020退货入库明细Q3.xlsx --database harmay --table refund_order_items --drop true
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/2020退货入库明细M10.xlsx --database harmay --table refund_order_items 

## 入库明细
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/2020采购入库明细Q3.xlsx --database harmay --table in_order_items --drop true
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/2020采购入库明细M10.xlsx --database harmay --table in_order_items

## 收钱吧刷卡支付
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/刷卡-上海新天地-M10.xlsx --database harmay --table shouqianba_bankcard --drop true --startRow 2
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/刷卡-上海浦深-M10.xlsx --database harmay --table shouqianba_bankcard --startRow 2
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/刷卡-北京浦深-M10.xlsx --database harmay --table shouqianba_bankcard --startRow 2
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/刷卡-北京贸易-M10.xlsx --database harmay --table shouqianba_bankcard --startRow 2
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/刷卡-成都浦深-M10.xlsx --database harmay --table shouqianba_bankcard --startRow 2
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/收钱吧刷卡-上海新天地-202009.xlsx --database harmay --table shouqianba_bankcard --startRow 2
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/收钱吧刷卡-北京浦深-202007.xlsx --database harmay --table shouqianba_bankcard --startRow 2
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/收钱吧刷卡-上海浦深-202008.xlsx --database harmay --table shouqianba_bankcard --startRow 2
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/收钱吧刷卡-上海浦深-202009.xlsx --database harmay --table shouqianba_bankcard --startRow 2
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/收钱吧刷卡-北京浦深-202007.xlsx --database harmay --table shouqianba_bankcard --startRow 2
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/收钱吧刷卡-北京浦深-202008.xlsx --database harmay --table shouqianba_bankcard --startRow 2
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/收钱吧刷卡-北京浦深-202009.xlsx --database harmay --table shouqianba_bankcard --startRow 2
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/收钱吧刷卡-北京贸易-202008.xlsx --database harmay --table shouqianba_bankcard --startRow 2
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/收钱吧刷卡-北京贸易-202009.xlsx --database harmay --table shouqianba_bankcard --startRow 2
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/收钱吧刷卡-成都浦深-202009.xlsx --database harmay --table shouqianba_bankcard --startRow 2

## 收钱吧移动
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/收钱吧移动交易Q3.xlsx --database harmay --table shouqianba_moblie --drop true
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/收钱吧移动交易明细M10.xlsx --database harmay --table shouqianba_moblie

## 旺POS
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/旺POS-上海浦深-202007-1.xlsx --database harmay --table wangpos --drop true  --sheetStart 2
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/旺POS-上海浦深-202007-2.xlsx --database harmay --table wangpos --sheetStart 2
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/旺POS-上海浦深-202007-3.xlsx --database harmay --table wangpos --sheetStart 2
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/旺POS-上海浦深-202007-4.xlsx --database harmay --table wangpos --sheetStart 2
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/旺POS-上海浦深-202007-5.xlsx --database harmay --table wangpos --sheetStart 2
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/旺POS-上海浦深-202008-1.xlsx --database harmay --table wangpos --sheetStart 2
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/旺POS-上海浦深-202008-2.xlsx --database harmay --table wangpos --sheetStart 2
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/旺POS-上海浦深-202008-3.xlsx --database harmay --table wangpos --sheetStart 2
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/旺POS-上海浦深-202008-4.xlsx --database harmay --table wangpos --sheetStart 2

## 通联支付

dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/通联支付-北京浦深-202007.xlsx --database harmay --table allinpay --drop true --startRow 2
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/通联支付-北京浦深-202008.xlsx --database harmay --table allinpay --startRow 2
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/通联支付-北京贸易-202007.xlsx --database harmay --table allinpay --startRow 2
dotnet run -s excel --file ~/Documents/Lewis/hicto/副本/话梅/data/通联支付-北京贸易-202008.xlsx --database harmay --table allinpay --startRow 2