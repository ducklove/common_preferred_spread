// js/calc.js — 지수·베타·SMA/EMA·mean/std·백분위·시총 계산
import { app, EMA_ALPHA, getRepresentativePairs } from './state.js';
import { toFiniteNumber } from './format.js';

export function getIndexPreferredMarketCap(pair) {
  const current = pair?.current || {};
  const preferredMarketCap = calculateLiveMarketCap(
    current.preferredPrice,
    current.preferredSharesOutstanding,
    current.preferredMarketCap,
  );
  return preferredMarketCap != null && preferredMarketCap > 0 ? preferredMarketCap : null;
}

export function calculateSimpleAverageMetrics(pairList = getRepresentativePairs()) {
  const sourcePairs = pairList.filter(pair => pair && !pair.isAverage && pair.current);
  const averageOf = (key) => {
    const values = sourcePairs
      .map(pair => toFiniteNumber(pair.current[key]))
      .filter(value => value != null);
    return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : null;
  };

  return {
    spread: averageOf('spread'),
    spreadChange: averageOf('spreadChange'),
    commonChange: averageOf('commonChange'),
    preferredChange: averageOf('preferredChange'),
    count: sourcePairs.length,
  };
}

export function calculateSqrtPreferredMarketCapSpreadIndex(pairList = app.pairs) {
  const issuers = new Map();

  pairList.forEach(pair => {
    if (!pair || pair.isAverage || !pair.current) return;
    const spread = toFiniteNumber(pair.current.spread);
    const preferredMarketCap = getIndexPreferredMarketCap(pair);
    if (spread == null || preferredMarketCap == null) return;

    const issuerName = pair.commonName || pair.name || pair.id;
    if (!issuers.has(issuerName)) {
      issuers.set(issuerName, {
        name: issuerName,
        totalMarketCap: 0,
        spreadWeighted: 0,
        spreadWeight: 0,
        spreadChangeWeighted: 0,
        spreadChangeWeight: 0,
        commonChangeWeighted: 0,
        commonChangeWeight: 0,
        preferredChangeWeighted: 0,
        preferredChangeWeight: 0,
        parts: [],
      });
    }

    const issuer = issuers.get(issuerName);
    issuer.totalMarketCap += preferredMarketCap;
    issuer.spreadWeighted += spread * preferredMarketCap;
    issuer.spreadWeight += preferredMarketCap;
    issuer.parts.push({
      id: pair.id,
      name: pair.name,
      preferredName: pair.preferredName || pair.name,
      preferredMarketCap,
      spread,
      spreadChange: toFiniteNumber(pair.current.spreadChange),
    });

    const spreadChange = toFiniteNumber(pair.current.spreadChange);
    if (spreadChange != null) {
      issuer.spreadChangeWeighted += spreadChange * preferredMarketCap;
      issuer.spreadChangeWeight += preferredMarketCap;
    }

    const commonChange = toFiniteNumber(pair.current.commonChange);
    if (commonChange != null) {
      issuer.commonChangeWeighted += commonChange * preferredMarketCap;
      issuer.commonChangeWeight += preferredMarketCap;
    }

    const preferredChange = toFiniteNumber(pair.current.preferredChange);
    if (preferredChange != null) {
      issuer.preferredChangeWeighted += preferredChange * preferredMarketCap;
      issuer.preferredChangeWeight += preferredMarketCap;
    }
  });

  const indexValues = {
    spreadWeighted: 0,
    spreadWeight: 0,
    spreadChangeWeighted: 0,
    spreadChangeWeight: 0,
    commonChangeWeighted: 0,
    commonChangeWeight: 0,
    preferredChangeWeighted: 0,
    preferredChangeWeight: 0,
    issuerCount: 0,
  };
  const constituents = [];

  issuers.forEach(issuer => {
    if (issuer.totalMarketCap <= 0 || issuer.spreadWeight <= 0) return;
    const indexWeight = Math.sqrt(issuer.totalMarketCap);
    if (!Number.isFinite(indexWeight) || indexWeight <= 0) return;

    const issuerSpread = issuer.spreadWeighted / issuer.spreadWeight;
    const issuerSpreadChange = issuer.spreadChangeWeight > 0
      ? issuer.spreadChangeWeighted / issuer.spreadChangeWeight
      : null;
    const parts = issuer.parts
      .slice()
      .sort((a, b) => b.preferredMarketCap - a.preferredMarketCap)
      .map(part => ({
        ...part,
        issuerShare: issuer.totalMarketCap > 0 ? part.preferredMarketCap / issuer.totalMarketCap * 100 : null,
      }));

    constituents.push({
      name: issuer.name,
      totalMarketCap: issuer.totalMarketCap,
      indexWeight,
      spread: issuerSpread,
      spreadChange: issuerSpreadChange,
      parts,
    });

    indexValues.issuerCount += 1;
    indexValues.spreadWeighted += issuerSpread * indexWeight;
    indexValues.spreadWeight += indexWeight;

    if (issuer.spreadChangeWeight > 0) {
      indexValues.spreadChangeWeighted += issuerSpreadChange * indexWeight;
      indexValues.spreadChangeWeight += indexWeight;
    }
    if (issuer.commonChangeWeight > 0) {
      indexValues.commonChangeWeighted += (issuer.commonChangeWeighted / issuer.commonChangeWeight) * indexWeight;
      indexValues.commonChangeWeight += indexWeight;
    }
    if (issuer.preferredChangeWeight > 0) {
      indexValues.preferredChangeWeighted += (issuer.preferredChangeWeighted / issuer.preferredChangeWeight) * indexWeight;
      indexValues.preferredChangeWeight += indexWeight;
    }
  });

  if (indexValues.spreadWeight <= 0) return null;
  constituents.forEach(item => {
    item.weight = item.indexWeight / indexValues.spreadWeight * 100;
  });
  constituents.sort((a, b) => b.weight - a.weight);

  return {
    spread: indexValues.spreadWeighted / indexValues.spreadWeight,
    spreadChange: indexValues.spreadChangeWeight > 0
      ? indexValues.spreadChangeWeighted / indexValues.spreadChangeWeight
      : null,
    commonChange: indexValues.commonChangeWeight > 0
      ? indexValues.commonChangeWeighted / indexValues.commonChangeWeight
      : null,
    preferredChange: indexValues.preferredChangeWeight > 0
      ? indexValues.preferredChangeWeighted / indexValues.preferredChangeWeight
      : null,
    issuerCount: indexValues.issuerCount,
    methodLabel: '제곱근 총액가중',
    constituents,
  };
}

export function calculatePairReturns(hist) {
  if (!Array.isArray(hist) || hist.length < 2) return null;

  const commonReturns = [];
  const preferredReturns = [];

  for (let i = 1; i < hist.length; i++) {
    const prev = hist[i - 1];
    const curr = hist[i];
    const prevCommon = prev?.commonPrice;
    const currCommon = curr?.commonPrice;
    const prevPreferred = prev?.preferredPrice;
    const currPreferred = curr?.preferredPrice;

    if (
      prevCommon == null || currCommon == null || prevPreferred == null || currPreferred == null
      || prevCommon <= 0 || currCommon <= 0 || prevPreferred <= 0 || currPreferred <= 0
    ) {
      continue;
    }

    commonReturns.push((currCommon - prevCommon) / prevCommon);
    preferredReturns.push((currPreferred - prevPreferred) / prevPreferred);
  }

  if (commonReturns.length < 2) return null;

  return { commonReturns, preferredReturns };
}

export function calculatePairBeta(hist) {
  const returns = calculatePairReturns(hist);
  if (!returns) return null;

  const { commonReturns, preferredReturns } = returns;
  const commonMean = commonReturns.reduce((sum, value) => sum + value, 0) / commonReturns.length;
  const preferredMean = preferredReturns.reduce((sum, value) => sum + value, 0) / preferredReturns.length;

  let covariance = 0;
  let variance = 0;

  for (let i = 0; i < commonReturns.length; i++) {
    const commonDelta = commonReturns[i] - commonMean;
    covariance += commonDelta * (preferredReturns[i] - preferredMean);
    variance += commonDelta * commonDelta;
  }

  if (variance === 0) return null;

  return covariance / variance;
}

export function calculatePairCorrelation(hist) {
  const returns = calculatePairReturns(hist);
  if (!returns) return null;

  const { commonReturns, preferredReturns } = returns;
  const commonMean = commonReturns.reduce((sum, value) => sum + value, 0) / commonReturns.length;
  const preferredMean = preferredReturns.reduce((sum, value) => sum + value, 0) / preferredReturns.length;

  let covariance = 0;
  let commonVariance = 0;
  let preferredVariance = 0;

  for (let i = 0; i < commonReturns.length; i++) {
    const commonDelta = commonReturns[i] - commonMean;
    const preferredDelta = preferredReturns[i] - preferredMean;
    covariance += commonDelta * preferredDelta;
    commonVariance += commonDelta * commonDelta;
    preferredVariance += preferredDelta * preferredDelta;
  }

  if (commonVariance === 0 || preferredVariance === 0) return null;

  return covariance / Math.sqrt(commonVariance * preferredVariance);
}

export function calculateEmaSeries(values, alpha = EMA_ALPHA) {
  if (!Array.isArray(values) || !values.length) return [];

  let ema = null;
  return values.map(value => {
    if (value == null || Number.isNaN(value)) return ema;
    ema = ema == null ? value : ((alpha * value) + ((1 - alpha) * ema));
    return ema;
  });
}

export function calculateLatestEma(values, alpha = EMA_ALPHA) {
  const series = calculateEmaSeries(values, alpha);
  for (let i = series.length - 1; i >= 0; i--) {
    const value = series[i];
    if (value != null && !Number.isNaN(value)) return value;
  }
  return null;
}

export function calculateSma(values, windowSize) {
  if (!Array.isArray(values) || !values.length) return null;
  const sliced = values.slice(-windowSize).filter(value => value != null && !Number.isNaN(value));
  if (!sliced.length) return null;
  return sliced.reduce((sum, value) => sum + value, 0) / sliced.length;
}

export function calculateSmaSeries(values, windowSize) {
  if (!Array.isArray(values) || !values.length) return [];

  const series = [];
  let rollingSum = 0;
  let validCount = 0;
  const queue = [];

  values.forEach(value => {
    const normalized = value == null || Number.isNaN(value) ? null : value;
    queue.push(normalized);
    if (normalized != null) {
      rollingSum += normalized;
      validCount += 1;
    }

    if (queue.length > windowSize) {
      const removed = queue.shift();
      if (removed != null) {
        rollingSum -= removed;
        validCount -= 1;
      }
    }

    series.push(validCount ? rollingSum / validCount : null);
  });

  return series;
}

export function calculateMeanStd(values) {
  if (!Array.isArray(values)) return null;
  const valid = values.filter(value => value != null && !Number.isNaN(value));
  if (valid.length < 2) return null;
  const mean = valid.reduce((sum, value) => sum + value, 0) / valid.length;
  const variance = valid.reduce((sum, value) => sum + ((value - mean) * (value - mean)), 0) / valid.length;
  return { mean, std: Math.sqrt(variance) };
}

export function calculatePercentileRank(values, current) {
  if (current == null || Number.isNaN(current) || !Array.isArray(values)) return null;
  const valid = values.filter(value => value != null && !Number.isNaN(value));
  if (!valid.length) return null;
  const below = valid.filter(value => value <= current).length;
  return below / valid.length * 100;
}

export function calculateLiveMarketCap(price, sharesOutstanding, fallbackMarketCap = null) {
  if (
    price != null && !Number.isNaN(price) && price > 0
    && sharesOutstanding != null && !Number.isNaN(sharesOutstanding) && sharesOutstanding > 0
  ) {
    return price * sharesOutstanding;
  }
  if (fallbackMarketCap != null && !Number.isNaN(fallbackMarketCap) && fallbackMarketCap > 0) {
    return fallbackMarketCap;
  }
  return null;
}

export function calculatePreferredRatio(commonMarketCap, preferredMarketCap) {
  if (
    commonMarketCap == null || Number.isNaN(commonMarketCap) || commonMarketCap <= 0
    || preferredMarketCap == null || Number.isNaN(preferredMarketCap) || preferredMarketCap < 0
  ) {
    return null;
  }
  return preferredMarketCap / commonMarketCap * 100;
}
