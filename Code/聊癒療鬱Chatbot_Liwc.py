# -*- coding: utf-8 -*-
"""
LIWC 中文文本情緒分析程式
功能: 從資料庫擷取聊天內容，分析用戶訊息的負面情緒詞彙比例
"""
import sys
import mysql.connector
import datetime
import pandas as pd
import re
import jieba
from tkinter import _flatten
from zhon.hanzi import punctuation


def main():
    """
    主程式：從資料庫獲取數據、處理聊天內容、判斷LIWC情緒值
    """
    # 設定使用者資訊並連接資料庫
    # userName = sys.argv[1]  # 命令行參數傳入使用者名稱
    userName = "77"  # 測試用固定使用者名稱
    userChat = userName + "chat"  # 使用者聊天資料表名稱
    
    # 建立資料庫連線
    conn = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        db="liaouapp",
        charset="utf8"
    )
    cursor = conn.cursor()
    
    # 從資料庫擷取聊天資料
    total = []
    cursor.execute(f"SELECT * FROM {userChat}")
    for row in cursor.fetchall():
        total.append(row)
    
    # 轉換為DataFrame並提取相關資訊
    total = pd.DataFrame(total)
    user = list(total[1])  # 發言者欄位
    content_total = list(total[2])  # 訊息內容欄位
    time_total = list(total[3])  # 時間欄位
    
    # 篩選出使用者的訊息內容及時間
    content = []
    time = []
    for r in range(0, len(user)):
        if user[r] == "user":
            content.append(content_total[r])
            time.append(time_total[r])
    
    # 根據時間間隔(30分鐘)分組聊天訊息
    useliwc = []  # 分組後的訊息內容
    timere = []   # 每組的開始時間
    count = 0
    useliwc.append([])
    timere.append(time[0])
    
    # 處理訊息分組
    while len(content) != 1:
        tt1 = str(time[0])
        tt2 = str(time[1])
        time_1 = datetime.datetime.strptime(tt1, "%Y-%m-%d %H:%M:%S")
        time_2 = datetime.datetime.strptime(tt2, "%Y-%m-%d %H:%M:%S")
        time_interval = time_2 - time_1
        diff_in_minutes = time_interval.total_seconds() / 60
        
        if round(diff_in_minutes) <= 30:
            # 時間間隔小於30分鐘，加入同一組
            useliwc[count].append(content[0])
            content.pop(0)
            time.pop(0) 
        elif round(diff_in_minutes) > 30:
            # 時間間隔大於30分鐘，創建新組
            useliwc[count].append(content[0])
            useliwc.append([])
            useliwc[count+1].append(content[1])
            content.pop(0)
            useliwc[count+1].pop(0)
            timere.append(time[1])
            time.pop(0)
            count += 1
    
    # 處理最後一筆訊息
    if round(diff_in_minutes) <= 30:
        useliwc[len(useliwc)-1].append(content[0])
        content.pop(0)
        time.pop(0) 
    elif round(diff_in_minutes) > 30:
        useliwc.pop()
        useliwc.append([content[0]])
    
    # 合併每組內的聊天內容
    for i in range(0, len(useliwc)):
        useliwc[i] = ''.join(useliwc[i])
    
    # 建立LIWC字典樹函數
    _end = "type"
    
    def make_trie(file):
        """
        建立字典樹 (Trie) 結構，用於快速詞彙查詢
        
        參數:
        file -- 包含詞彙和類型的列表
        
        返回:
        root -- 字典樹根節點
        """
        root = dict()
        for array in file:
            current_dict = root
            for x in array[0]:
                current_dict = current_dict.setdefault(x, {})
            current_dict[_end] = array[1:]
        return root
    
    # 載入LIWC字典檔案
    f_liwc = open("C:\\Users\\User\\Desktop\\聊癒療鬱\\0318_KNN_Chi\\Jieba\\cliwc_all.dic", "r", encoding="utf-8")
    file = []
    for row_liwc in f_liwc:
        file.append(row_liwc.split())
    f_liwc.close()
    
    # 建立字典樹
    dct = make_trie(file)
    
    def wordtype(word, trie=dct):
        """
        查詢詞彙在LIWC中的類型
        
        參數:
        word -- 要查詢的詞彙
        trie -- 字典樹
        
        返回:
        該詞彙的類型列表，若不存在則返回['0']
        """
        current_dict = trie
        n = ['0']
        for letter in word:
            if letter not in current_dict:
                return n
            current_dict = current_dict[letter]
        if "type" in current_dict:
            return current_dict["type"]
        else:
            return n
    
    def add(trie, word):
        """
        向字典樹添加新詞彙
        
        參數:
        trie -- 字典樹
        word -- 要添加的詞彙
        
        返回:
        更新後的字典樹
        """
        current_dict = trie
        for x in word:
            current_dict = current_dict.setdefault(x, {})
        current_dict[_end] = _end
        return trie
    
    # 設定jieba分詞
    jieba.set_dictionary('C:\\Users\\User\\Desktop\\dict1.txt')
    
    # 載入需要切分的詞彙
    f_becut = open("C:\\Users\\User\\Desktop\\liwc.txt", "r", encoding="utf-8")
    becut = []
    for row_becut in f_becut:
        becut.append(row_becut.split())
    f_becut.close()
    
    # 將詞彙添加到jieba的自定義字典
    for i in range(0, len(becut)):
        becut[i] = ''.join(becut[i])
        jieba.add_word(becut[i][0])
    
    # 處理倒數第二組訊息內容（front_message）
    front_message = useliwc[-2]
    
    # 計算特殊表情符號數量
    hahacount = front_message.count("ㄏㄏ")
    front_message = front_message.replace("ㄏㄏ", "")
    
    # 文本清理：移除標點符號、英文和數字
    front_message = re.sub(r"([^\u4e00-\u9fa5\u0030-\u0039\u0041-\u005a\u0061-\u007a])", "", front_message)  # 去標點
    front_message = re.sub('[a-zA-Z]', '', front_message)  # 去英文
    front_message = re.sub('[\d]', '', front_message)  # 去數字
    
    # 使用jieba進行分詞
    front_message = jieba.lcut(front_message)
    
    # 載入單字LIWC詞典
    liwconedict = open("C:\\Users\\User\\Desktop\\liwcone.txt", "r", encoding="utf-8")
    liwcone = []
    for row in liwconedict:
        liwcone.append(row.split())
    liwconedict.close()
    liwcone = list(_flatten(liwcone))
    
    # 分析負面情緒詞彙
    nega_word = []  # 負面情緒詞彙列表
    liwc_type = []  # 詞彙類型列表
    
    # 獲取每個詞的LIWC類型
    for i in range(0, len(front_message)):
        liwc_type.append(wordtype(front_message[i]))
    
    # 找出負面情緒詞彙（類型50777或50778）
    for j in range(0, len(liwc_type)):
        for z in range(0, len(liwc_type[j])):
            if (liwc_type[j][z] == "50777" or liwc_type[j][z] == "50778"):
                nega_word.append(front_message[j])
    
    # 處理未識別詞彙(liwcone)的LIWC類型
    liwc_type_one = []
    liwc_type_one_word = []
    for i in range(0, len(liwc_type)):
        for j in range(0, len(liwc_type[i])):
            for k in range(0, len(liwcone)):
                if liwc_type[i][j] == "0" and liwcone[k] in front_message[i]:
                    liwc_type_one.append(wordtype(liwcone[k]))
                    liwc_type_one_word.append(liwcone[k])
                    front_message.append(liwcone[k])
    
    # 從liwcone中找出負面情緒詞彙
    for i in range(0, len(liwc_type_one)):
        for j in range(0, len(liwc_type_one[i])):
            if liwc_type_one[i][j] == "50777" or liwc_type_one[i][j] == "50778":
                nega_word.append(liwc_type_one_word[i])
    
    # 過濾掉特定類型的詞彙
    for i in range(0, len(liwc_type)):
        for j in range(0, len(liwc_type[i])):
            # 過濾類型為0, 3, 9, 12, 16, 17, 18, 19, 34的詞彙
            if (liwc_type[i][j] == "0" or liwc_type[i][j] == "3" or 
                liwc_type[i][j] == "9" or liwc_type[i][j] == "12" or 
                liwc_type[i][j] == "16" or liwc_type[i][j] == "17" or
                liwc_type[i][j] == "18" or liwc_type[i][j] == "19" or 
                liwc_type[i][j] == "34"):
                front_message[i] = ""
    
    # 移除空白內容
    front_message = [x.strip() for x in front_message if x.strip() != '']
    
    # 計算總詞數（包括表情符號）和負面情緒詞數
    totalword = len(front_message) + hahacount
    totalneg = len(nega_word)
    
    # 判斷負面情緒詞比例是否超過閾值(0.056)
    if (totalneg / totalword) > 0.0560:
        b = "Y"  # 負面情緒標記
        # 更新資料庫用戶的LIWC狀態
        cursor.execute(f"UPDATE register SET LIWC='{b}' WHERE username='{userName}'")
    
    # 提交資料庫變更並關閉連線
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == '__main__':
    main()