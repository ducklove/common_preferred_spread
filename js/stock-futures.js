// 추적 대상 본주 중 개별주식선물 기초자산으로 확인된 종목.
// KIS fo_stk_code_mts.mst: 상품종류 1/3(선물), 기초자산 단축코드 기준.
// 2026-10-15 조회 당시 2026-11 이후 만기가 존재하는 종목만 포함한다.
// 옵션/스프레드 및 만기가 지난 계약만 남은 종목은 제외한다.
export const STOCK_FUTURES_AS_OF = '2026-10-15';
export const STOCK_FUTURES_SOURCE_URL = 'https://new.real.download.dws.co.kr/common/master/fo_stk_code_mts.mst.zip';

const COMMON_STOCK_FUTURES_CODES = new Set([
  '000100', // 유한양행
  '000150', // 두산
  '000720', // 현대건설
  '000810', // 삼성화재
  '000880', // 한화
  '001040', // CJ
  '002790', // 아모레퍼시픽홀딩스
  '003550', // LG
  '005300', // 롯데칠성
  '005380', // 현대차
  '005930', // 삼성전자
  '005940', // NH투자증권
  '006400', // 삼성SDI
  '006800', // 미래에셋증권
  '009150', // 삼성전기
  '009830', // 한화솔루션
  '010950', // S-Oil
  '011780', // 금호석유
  '028260', // 삼성물산
  '034730', // SK
  '051900', // LG생활건강
  '051910', // LG화학
  '066570', // LG전자
  '071050', // 한국금융지주
  '078930', // GS
  '090430', // 아모레퍼시픽
  '096770', // SK이노베이션
  '097950', // CJ제일제당
  '120110', // 코오롱인더
  '180640', // 한진칼
  '285130', // SK케미칼
  '375500', // DL이앤씨
]);

export function hasCommonStockFutures(ticker) {
  const code = String(ticker || '').trim().replace(/\.(KS|KQ)$/i, '');
  return COMMON_STOCK_FUTURES_CODES.has(code);
}
