import type {
  Platform,
  PlaylistData,
  Quality,
  SearchResult,
  Song,
  SongInfo,
  ToplistSummary,
} from './types';
import {
  CACHE_TTL,
  generateCacheKey,
  getFromCache,
  saveToCache,
} from './utils/cache';

// TuneHub V3 API Configuration
const API_BASE = 'https://tunehub.sayqz.com/api';
const API_KEY = 'th_d83c68b0945ec6126c4413907c1c772b7bb7de08585b47f5';

const PARSE_API_BASES = [
  `${API_BASE}/v1/parse`,
];

const SEARCH_API_BASES = [
  { base: API_BASE, tag: 'tunehub' },
];

const PARSE_TIMEOUT_MS = 6000;

// Common headers for API requests
const getHeaders = () => {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
  };
  if (API_KEY) {
    headers['X-API-Key'] = API_KEY;
    headers['Authorization'] = `Bearer ${API_KEY}`;
  }
  return headers;
};

// Parse API response
const handleResponse = async <T>(response: Response): Promise<T> => {
  if (!response.ok) {
    const contentType = response.headers.get('content-type');
    let message = `Request failed with ${response.status}`;

    try {
      if (contentType?.includes('application/json')) {
        const data = await response.json();
        message = data.message || data.error || message;
      } else {
        const text = await response.text();
        // If response is HTML (like a 404 page), extract a meaningful error
        if (text.includes('<!DOCTYPE html>') || text.includes('<html')) {
          message = `API endpoint not found (${response.status})`;
        } else {
          message = text.substring(0, 200) || message;
        }
      }
    } catch {
      // If parsing fails, use the default message
    }

    throw new Error(message);
  }

  const data = await response.json();
  const code = data.code;
  const isSuccessCode = code === undefined || code === 0 || code === 200 || code === '0' || code === '200';
  if (!isSuccessCode && data.success !== true) {
    throw new Error(data.message || `API Error${code !== undefined ? ` (${code})` : ''}`);
  }
  return data as T;
};

const fetchText = async (url: string): Promise<string> => {
  const response = await fetch(url);
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || `Request failed with ${response.status}`);
  }
  return response.text();
};

// --- Template and Transform Engines ---

const evaluateTemplate = (template: string, variables: Record<string, any>): any => {
  if (!template) return '';
  if (typeof template !== 'string') return template;
  if (!template.includes('{{')) return template;

  const toNumber = (val: any, fallback: number) => {
    const num = Number(val);
    return Number.isFinite(num) ? num : fallback;
  };

  const page = toNumber(variables.page, 1);
  const limit = toNumber(variables.limit ?? variables.pageSize, toNumber(variables.pageSize, 20));
  const keyword = variables.keyword ?? '';

  const compute = (expr: string): any => {
    const e = expr.trim();
    if (e === 'keyword') return keyword;
    if (e === 'page' || e === 'page || 1' || e === '(page || 1)') return page;
    if (e === 'limit' || e === 'limit || 20' || e === '(limit || 20)') return limit;
    if (e === 'pageSize' || e === 'pageSize || limit || 20' || e === '(pageSize || limit || 20)') {
      return toNumber(variables.pageSize, limit);
    }
    if (e === '(page || 1) - 1' || e === '((page || 1) - 1)') return page - 1;
    if (e === '((page || 1) - 1) * (limit || 20)' || e === '(page - 1) * limit') {
      return (page - 1) * limit;
    }
    const fallbackMatch = e.match(/^(\w+)\s*\|\|\s*(.+)$/);
    if (fallbackMatch) {
      const [, name, defRaw] = fallbackMatch;
      const val = variables[name];
      if (val !== undefined && val !== null && val !== '') return val;
      const defNum = Number(defRaw);
      if (Number.isFinite(defNum)) return defNum;
      return defRaw.replace(/^['"]|['"]$/g, '');
    }
    return variables[e] ?? '';
  };

  const fullMatch = template.trim().match(/^\{\{\s*(.+?)\s*\}\}$/);
  if (fullMatch) {
    return compute(fullMatch[1]);
  }

  return template.replace(/\{\{\s*(.+?)\s*\}\}/g, (_, expr) => {
    const result = compute(expr);
    return result === undefined || result === null ? '' : String(result);
  });
};

// --- Normalization Helpers (Aligned with Cloudflare Worker) ---

const formatArtist = (value: any): string => {
  if (!value) return '';
  if (typeof value === 'string') return value.replace(/&/g, ', ');
  if (Array.isArray(value)) return value.map(formatArtist).filter(Boolean).join(', ');
  if (typeof value === 'object') {
    return value.name || value.artist || value.artistName || '';
  }
  return '';
};

const buildQQAlbumPic = (mid?: string) => (mid ? `https://y.gtimg.cn/music/photo_new/T002R300x300M000${mid}.jpg` : '');

const resolveAlbumPic = (item: any) => {
  const pic = item.pic || item.cover || item.picUrl || item.coverImg ||
    item.albumpic || item.albumpic_big || item.albumPic || item.album_pic ||
    item.album?.picUrl || item.album?.pic || item.album?.cover || item.album?.img ||
    item.web_albumpic_short || item.web_albumpic || item.hts_MVPIC || item.MVPIC ||
    item.web_artistpic_short || item.pic120 ||
    (item.album?.mid ? buildQQAlbumPic(item.album.mid) : '') ||
    (item.albumMid ? buildQQAlbumPic(item.albumMid) : '') ||
    (item.albumpic_big ? item.albumpic_big : '');

  if (typeof pic === 'string' && pic.startsWith('120/')) {
    return `https://img4.kuwo.cn/star/albumcover/${pic.substring(4)}`;
  }
  return pic;
};

const normalizeSong = (item: any, platform: Platform): Song => {
  const idValue = item.mid ??
    item.songmid ??
    item.songMid ??
    item.strMediaMid ??
    item.file?.media_mid ??
    item.songId ??
    item.songid ??
    item.id ??
    item.musicId ??
    item.musicid ??
    item.MUSICRID ??
    item.rid ??
    item.musicrid;
  const id = typeof idValue === 'string' ? idValue.replace(/^MUSIC_/, '') : String(idValue || '');
  const name = item.name ?? item.songname ?? item.songName ?? item.title ?? item.SONGNAME ?? 'Unknown Song';

  // Robust artist extraction
  let artistValue = item.artist ?? item.artists ?? item.singer ?? item.ARTIST ?? item.artistname ?? item.artistName ?? item.author;
  if (!artistValue && item.singer && Array.isArray(item.singer)) {
    artistValue = item.singer;
  }
  const artist = formatArtist(artistValue);

  const album = item.album?.name ?? item.albumName ?? item.ALBUM ?? item.album ?? '';
  const pic = resolveAlbumPic(item);

  return {
    id: id ? String(id) : '',
    name: typeof name === 'string' ? name : '',
    artist,
    album: typeof album === 'string' ? album : (item.album?.title || ''),
    pic: typeof pic === 'string' ? pic : '',
    platform,
  };
};

const extractSongsList = (data: any): any[] => {
  if (!data) return [];
  if (Array.isArray(data)) return data;
  if (Array.isArray(data.results)) return data.results;
  if (Array.isArray(data.list)) return data.list;
  if (Array.isArray(data.songs)) return data.songs;
  if (Array.isArray(data.abslist)) return data.abslist; // Kuwo specific

  if (data.data) {
    if (Array.isArray(data.data)) return data.data;
    if (Array.isArray(data.data.results)) return data.data.results;
    if (Array.isArray(data.data.list)) return data.data.list;
    if (Array.isArray(data.data.songs)) return data.data.songs;
    if (Array.isArray(data.data.abslist)) return data.data.abslist;
    // QQ search.data.body.song.list
    if (data.data.body?.song?.list) return data.data.body.song.list;
    if (data.data.body?.song?.songlist) return data.data.body.song.songlist;
  }

  if (data.result) {
    if (Array.isArray(data.result.songs)) return data.result.songs;
    if (Array.isArray(data.result.song)) return data.result.song;
  }

  // Deep paths for QQ/etc components
  const root = data.req || data.search || data;
  if (root?.data?.body?.song?.list) return root.data.body.song.list;
  if (root?.data?.body?.song?.songlist) return root.data.body.song.songlist;
  if (root?.data?.body?.item_list) return root.data.body.item_list;
  if (root?.data?.results) return root.data.results;

  return [];
};

/**
 * Extract tracks from playlist-specific API responses.
 * Handles playlist data structures that differ from search results.
 */
const extractPlaylistTracks = (data: any, platform: Platform): any[] => {
  if (!data) return [];

  // Netease: playlist.tracks is the primary path
  if (platform === 'netease') {
    const candidates = [
      data.playlist?.tracks,
      data.data?.playlist?.tracks,
      data.result?.playlist?.tracks,
      data.result?.tracks,
      data.tracks,
      data.playlist?.trackIds,
    ];
    for (const cand of candidates) {
      if (Array.isArray(cand) && cand.length) return cand;
    }
  }

  // Kuwo: musiclist is the primary path for playlists
  if (platform === 'kuwo') {
    const candidates = [
      data.musiclist,
      data.data?.musiclist,
      data.data?.musicList,
      data.musicList,
    ];
    for (const cand of candidates) {
      if (Array.isArray(cand) && cand.length) return cand;
    }
  }

  // QQ: many possible nested paths
  if (platform === 'qq') {
    const candidates = [
      data.req_0?.data?.songlist,
      data.req?.data?.songlist,
      data.data?.songlist,
      data.songlist,
      data.req_0?.data?.songList,
      data.req?.data?.songList,
      data.data?.songList,
      data.songList,
      data.cdlist?.[0]?.songlist,
      data.cdlist?.[0]?.songList,
      data.req_0?.data?.songInfoList,
      data.req?.data?.songInfoList,
      data.data?.songInfoList,
      data.songInfoList,
      data.req_0?.data?.song,
      data.req?.data?.song,
      data.data?.song,
      data.song,
      data.req_0?.data?.track_info?.list,
      data.req?.data?.track_info?.list,
      data.data?.track_info?.list,
    ];
    for (const cand of candidates) {
      if (Array.isArray(cand) && cand.length) return cand;
    }
  }

  // Fall back to generic search extraction
  return extractSongsList(data);
};




// --- Method Delivery Logic ---

interface MethodConfig {
  type: string;
  method: 'GET' | 'POST';
  url: string;
  params?: Record<string, string>;
  body?: Record<string, unknown>;
  headers?: Record<string, string>;
}

const getMethodConfig = async (platform: Platform, func: string): Promise<MethodConfig> => {
  const response = await fetch(`${API_BASE}/v1/methods/${platform}/${func}`, {
    headers: getHeaders(),
  });
  const res = await handleResponse<{ data: MethodConfig }>(response);
  return res.data;
};


const executeMethod = async (config: MethodConfig, variables: Record<string, any>): Promise<any> => {
  let url = config.url;
  const params = config.params ? { ...config.params } : undefined;
  const body = config.body ? JSON.parse(JSON.stringify(config.body)) : undefined;

  // Substitute variables in URL
  url = evaluateTemplate(url, variables);

  // Substitute variables in Params
  if (params) {
    for (const [key, value] of Object.entries(params)) {
      if (typeof value === 'string') {
        params[key] = String(evaluateTemplate(value, variables));
      }
    }
  }

  // Substitute variables in Body (preserving types)
  if (body) {
    const processObj = (obj: any) => {
      if (!obj || typeof obj !== 'object') return;
      for (const k in obj) {
        if (typeof obj[k] === 'string') {
          obj[k] = evaluateTemplate(obj[k], variables);
        } else if (typeof obj[k] === 'object') {
          processObj(obj[k]);
        }
      }
    };
    processObj(body);

    // QQ Music specific overrides - ensure mobile protocol
    if (config.url.includes('y.qq.com') && body.comm) {
      body.comm.ct = 11;
      body.comm.cv = 0;
    }
  }

  // Build final URL
  if (params && Object.keys(params).length > 0) {
    const searchParams = new URLSearchParams();
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined && value !== null && value !== '') {
        searchParams.append(key, String(value));
      }
    }
    const query = searchParams.toString();
    if (query) {
      url = url.includes('?') ? `${url}&${query}` : `${url}?${query}`;
    }
  }

  // CORS/Forbidden Header Filtering for Browser
  const forbiddenHeaders = ['user-agent', 'referer', 'content-length', 'host'];
  const safeHeaders: Record<string, string> = {};
  if (config.headers) {
    for (const [k, v] of Object.entries(config.headers)) {
      if (!forbiddenHeaders.includes(k.toLowerCase())) {
        safeHeaders[k] = v;
      }
    }
  }

  const fetchOptions: RequestInit = {
    method: config.method,
    headers: safeHeaders
  };

  if (body && config.method === 'POST') {
    fetchOptions.body = JSON.stringify(body);
    if (!safeHeaders['Content-Type']) {
      (fetchOptions.headers as any)['Content-Type'] = 'application/json';
    }
  }

  try {
    const response = await fetch(url, fetchOptions);
    if (!response.ok) {
      const text = await response.text();
      // If we are in browser and direct fetch failed (likely CORS), we log it as advice
      if (typeof window !== 'undefined') {
        console.warn(`Direct fetch to ${url} failed. This is expected in browsers due to CORS. Routing via proxy...`);
      }
      throw new Error(`Upstream HTTP ${response.status}: ${text.substring(0, 100)}`);
    }
    return await response.json();
  } catch (err) {
    throw err;
  }
};

// --- Cover Augmentation Helpers ---

const determineTotal = (data: any, fallback: number) => {
  if (!data || typeof data !== 'object') return fallback;
  if (typeof data.total === 'number') return data.total;
  if (typeof data.totalCount === 'number') return data.totalCount;
  if (typeof data.songCount === 'number') return data.songCount;
  if (typeof data.totalnum === 'number') return data.totalnum;

  if (data.data && typeof data.data === 'object') {
    const nested = data.data;
    if (typeof nested.total === 'number') return nested.total;
    if (typeof nested.totalnum === 'number') return nested.totalnum;
    if (typeof nested.songCount === 'number') return nested.songCount;
    if (nested.meta && typeof nested.meta.sum === 'number') return nested.meta.sum; // QQ meta.sum
  }

  const root = data.req || data.search || data;
  return root?.data?.body?.song?.totalnum ??
    root?.data?.body?.song?.total_num ??
    root?.data?.meta?.sum ??
    fallback;
};


// --- Core API Functions ---

interface ParseResultItem {
  id: string;
  success: boolean;
  url?: string;
  info?: {
    name: string;
    artist: string;
    album: string;
    duration?: number;
  };
  cover?: string;
  lyrics?: string;
}

interface ParseResponse {
  code: number;
  success: boolean;
  data: {
    data: ParseResultItem[];
  };
}

interface NormalizedParseResult {
  id: string;
  name: string;
  artist: string;
  album: string;
  url: string;
  pic: string;
  lrc: string;
}

export const parseSongs = async (
  platform: Platform,
  ids: string | string[],
  quality: Quality = '320k'
): Promise<NormalizedParseResult[]> => {
  const idsStr = Array.isArray(ids) ? ids.join(',') : ids;

  const errors: string[] = [];
  const qualityCandidates: Quality[] = Array.from(new Set([quality, '128k'])) as Quality[];

  for (const base of PARSE_API_BASES) {
    for (const q of qualityCandidates) {
      try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), PARSE_TIMEOUT_MS);

        const response = await fetch(base, {
          method: 'POST',
          headers: getHeaders(),
          body: JSON.stringify({
            platform,
            ids: idsStr,
            quality: q,
          }),
          signal: controller.signal,
        });
        clearTimeout(timeout);
        const payload = await handleResponse<ParseResponse>(response);
        if (payload?.data?.data?.length) {
          return payload.data.data
            .filter(item => item.success && item.url)
            .map(item => ({
              id: item.id,
              name: item.info?.name || '',
              artist: item.info?.artist || '',
              album: item.info?.album || '',
              url: item.url || '',
              pic: item.cover || '',
              lrc: item.lyrics || '',
            }));
        }
      } catch (error: any) {
        errors.push(`${base}: ${error?.message || error}`);
        console.error('Parse API error:', error);
      }
    }
  }

  console.error('Parse API failed:', errors.join(' | '));
  return [];
};

export const getParsedUrl = async (
  source: Platform,
  id: string,
  quality: Quality = '320k'
): Promise<string> => {
  const cacheKey = generateCacheKey('parseurl', source, id, quality);
  const cached = getFromCache<string>(cacheKey);
  if (cached) return cached;

  // Browser: use our worker proxy to avoid CORS/key exposure
  if (typeof window !== 'undefined') {
    try {
      const params = new URLSearchParams({
        type: 'song',
        source,
        id,
        quality,
      });
      const response = await fetch(`/api?${params.toString()}`);
      const payload = await handleResponse<{ data?: { url?: string } }>(response);
      const url = payload.data?.url;
      if (url) {
        saveToCache(cacheKey, url, CACHE_TTL.SONG_INFO);
        return url;
      }
    } catch (err) {
      console.error('Browser parse proxy failed:', err);
    }
  }

  const results = await parseSongs(source, id, quality);
  if (results.length > 0 && results[0].url) {
    saveToCache(cacheKey, results[0].url, CACHE_TTL.SONG_INFO);
    return results[0].url;
  }
  throw new Error('Failed to load song URL');
};

const fetchTunehubSearch = async (
  source: Platform,
  keyword: string,
  limit: number,
  page: number
): Promise<SearchResult> => {
  const commonParams = {
    source,
    platform: source,
    keyword,
    limit: String(limit),
    page: String(page),
    type: 'search',
  };

  let fallbackResult: SearchResult | null = null;
  const errors: string[] = [];

  for (const { base, tag } of SEARCH_API_BASES) {
    try {
      const attempts = [
        { path: `${base}/v1/search`, params: commonParams },
        { path: `${base}/search`, params: commonParams },
        { path: `${base}/`, params: { ...commonParams, type: 'search' } },
      ];

      for (const attempt of attempts) {
        const searchParams = new URLSearchParams();
        Object.entries(attempt.params).forEach(([k, v]) => {
          if (v !== undefined && v !== null && v !== '') searchParams.append(k, String(v));
        });
        const url = attempt.path + (attempt.path.includes('?') ? '&' : '?') + searchParams.toString();

        try {
          const response = await fetch(url, { headers: getHeaders() });
          const payload = await handleResponse<{ data?: any }>(response);
          const searchData = payload.data ?? payload;
          const rawList = extractSongsList(searchData);
          const songs = rawList.map(item => normalizeSong(item, source));
          const total = determineTotal(searchData, songs.length);

          const result: SearchResult = { keyword, limit, page, total, results: songs };

          if (songs.length > 0) {
            return result;
          }
          if (!fallbackResult) {
            fallbackResult = result;
          }
        } catch (error: any) {
          errors.push(`${tag} ${attempt.path}: ${error?.message || error}`);
          console.warn(`Search via ${tag} failed:`, error);
        }
      }
    } catch (error: any) {
      errors.push(`${tag}: ${error?.message || error}`);
      console.warn(`Search via ${tag} failed:`, error);
    }
  }

  if (fallbackResult) return fallbackResult;

  console.error(`Search error for ${source}:`, errors.join(' | '));
  return { keyword, limit, page, total: 0, results: [] };
};

export const searchSongs = async (
  source: Platform,
  keyword: string,
  limit = 30,
  page = 1,
): Promise<SearchResult> => {
  // Browser-side proxy relay
  if (typeof window !== 'undefined') {
    try {
      const params = new URLSearchParams({
        type: 'search',
        source,
        keyword,
        limit: String(limit),
        page: String(page)
      });
      const response = await fetch(`/api?${params}`);
      const data = await handleResponse<{ data: SearchResult }>(response);
      return data.data;
    } catch (error) {
      console.error(`Browser search relay error for ${source}:`, error);
    }
  }

  try {
    const result = await fetchTunehubSearch(source, keyword, limit, page);
    return result;
  } catch (error) {
    console.error(`Search error for ${source}:`, error);
    return { keyword, limit, page, total: 0, results: [] };
  }
};

export const getSongInfo = async (source: Platform, id: string): Promise<SongInfo> => {
  const cacheKey = generateCacheKey('songinfo', source, id);
  const cached = getFromCache<SongInfo>(cacheKey);
  if (cached) return cached;

  const results = await parseSongs(source, id);
  if (results.length > 0) {
    const info: SongInfo = {
      name: results[0].name,
      artist: results[0].artist,
      album: results[0].album,
      url: results[0].url,
      pic: results[0].pic,
      lrc: results[0].lrc,
    };
    saveToCache(cacheKey, info, CACHE_TTL.SONG_INFO);
    return info;
  }
  throw new Error('Song info not found');
};

export const getLyrics = async (source: Platform, id: string, lrcUrl?: string): Promise<string> => {
  const cacheKey = generateCacheKey('lyrics', source, id);
  const cached = getFromCache<string>(cacheKey);
  if (cached) return cached;

  if (lrcUrl) {
    try {
      const lyrics = await fetchText(lrcUrl);
      if (lyrics) {
        saveToCache(cacheKey, lyrics, CACHE_TTL.LYRICS);
        return lyrics;
      }
    } catch { /* ignore */ }
  }

  try {
    const results = await parseSongs(source, id);
    if (results.length > 0 && results[0].lrc) {
      const lyrics = results[0].lrc.startsWith('http')
        ? await fetchText(results[0].lrc)
        : results[0].lrc;

      if (lyrics) {
        saveToCache(cacheKey, lyrics, CACHE_TTL.LYRICS);
        return lyrics;
      }
    }
  } catch { /* ignore */ }

  return '';
};

export const getPlaylist = async (source: Platform, id: string): Promise<PlaylistData> => {
  const cacheKey = generateCacheKey('playlist', source, id);
  const cached = getFromCache<PlaylistData>(cacheKey);
  if (cached) return cached;

  // Browser-side proxy relay
  if (typeof window !== 'undefined') {
    try {
      const params = new URLSearchParams({ type: 'playlist', source, id });
      const response = await fetch(`/api?${params}`);
      const data = await handleResponse<{ data: PlaylistData }>(response);
      saveToCache(cacheKey, data.data, CACHE_TTL.PLAYLIST);
      return data.data;
    } catch (error) {
      console.error(`Browser playlist relay error for ${source}:`, error);
    }
  }

  try {
    const config = await getMethodConfig(source, 'playlist');
    const rawData = await executeMethod(config, { id, limit: 9999, pageSize: 9999 });

    const rawList = extractPlaylistTracks(rawData, source);
    const songs = rawList.map(item => normalizeSong(item, source));

    const info = rawData.playlist || rawData.data?.playlist || rawData.info || rawData.data?.info || { name: 'Playlist' };
    const result: PlaylistData = {
      info: {
        name: info.name || info.dissname || 'Playlist',
        pic: info.coverImgUrl || info.pic || info.logo || info.img || '',
        desc: info.description || info.desc || info.info || '',
        author: info.creator?.nickname || info.author || info.nickname || '',
        playCount: info.playCount || info.playnum || info.visitnum || 0,
      },
      list: songs,
    };

    saveToCache(cacheKey, result, CACHE_TTL.PLAYLIST);
    return result;
  } catch (error) {
    console.error(`Playlist error for ${source}:`, error);
    return { info: { name: 'Unknown Playlist' }, list: [] };
  }
};



export const getToplists = async (source: Platform): Promise<ToplistSummary[]> => {
  const cacheKey = generateCacheKey('toplists', source);
  const cached = getFromCache<ToplistSummary[]>(cacheKey);
  if (cached) return cached;

  // Browser-side proxy relay
  if (typeof window !== 'undefined') {
    try {
      const params = new URLSearchParams({ type: 'toplists', source });
      const response = await fetch(`/api?${params}`);
      const data = await handleResponse<{ data: { list: ToplistSummary[] } }>(response);
      saveToCache(cacheKey, data.data.list, CACHE_TTL.TOPLISTS);
      return data.data.list;
    } catch (error) {
      console.error(`Browser toplists relay error for ${source}:`, error);
    }
  }

  try {
    const config = await getMethodConfig(source, 'toplists');
    const rawData = await executeMethod(config, {});

    const list = (rawData.list || rawData.data?.list || rawData || []).map((item: any) => ({
      id: String(item.id ?? item.topId),
      name: String(item.name ?? item.title),
      pic: resolveAlbumPic(item),
      updateFrequency: item.updateFrequency,
      desc: item.desc ?? item.description,
      platform: source,
    }));

    saveToCache(cacheKey, list, CACHE_TTL.TOPLISTS);
    return list;
  } catch (error) {
    console.error(`Toplists error for ${source}:`, error);
    return [];
  }
};

export const getToplistSongs = async (
  source: Platform,
  id: string,
): Promise<{ list: Song[]; source: Platform }> => {
  const cacheKey = generateCacheKey('toplistsongs', source, id);
  const cached = getFromCache<{ list: Song[]; source: Platform }>(cacheKey);
  if (cached) return cached;

  // Browser-side proxy relay
  if (typeof window !== 'undefined') {
    try {
      const params = new URLSearchParams({ type: 'toplist', source, id });
      const response = await fetch(`/api?${params}`);
      const data = await handleResponse<{ data: { source: Platform, list: Song[] } }>(response);
      saveToCache(cacheKey, data.data, CACHE_TTL.TOPLIST_SONGS);
      return data.data;
    } catch (error) {
      console.error(`Browser toplist songs relay error for ${source}:`, error);
    }
  }

  try {
    const config = await getMethodConfig(source, 'toplist');
    const rawData = await executeMethod(config, { id });

    const rawList = extractSongsList(rawData);
    const songs = rawList.map(item => normalizeSong(item, source));

    const result = { source, list: songs };
    saveToCache(cacheKey, result, CACHE_TTL.TOPLIST_SONGS);
    return result;
  } catch (error) {
    console.error(`Toplist songs error for ${source}:`, error);
    return { source, list: [] };
  }
};

// --- Dummy implementation for remaining exports ---
export const buildFileUrl = (_source: Platform, _id: string, _type: 'url' | 'pic' | 'lrc', _quality?: Quality): string => '';
export const cacheSearchResults = async (source: Platform, payload: SearchResult): Promise<void> => {
  if (typeof window === 'undefined') return;
  try {
    const params = new URLSearchParams({ type: 'cache-search', source });
    await fetch(`/api?${params.toString()}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(payload),
    });
  } catch (error) {
    console.warn('Failed to sync search cache:', error);
  }
};

export { clearAllCache, getCacheStats, cleanupExpiredCache } from './utils/cache';
