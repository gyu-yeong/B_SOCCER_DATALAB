# B_Soccer_DataLab

K리그 데이터 분석 플랫폼

## 개발 환경 설정
```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

## 실행
```bash
streamlit run app/main.py
```
```
**4. 저장**

---

## 💡 팁: 점(.) 파일이 안 보일 때

Windows에서 `.gitignore` 같은 파일이 안 보이면:

**파일 탐색기에서:**
1. `보기` 탭 클릭
2. `숨김 항목` 체크

**VSCode에서는 자동으로 보여요!**

---

## ✅ 완료 확인

VSCode Explorer에서 이렇게 보이면 성공:
```
b_soccer_datalab/
├── .env              ← 회색 글씨
├── .gitignore        ← 회색 글씨
├── README.md
├── requirements.txt
├── venv/             ← 흐릿하게 표시
├── data/
│   ├── raw/
│   └── processed/
├── database/
├── scripts/
└── app/