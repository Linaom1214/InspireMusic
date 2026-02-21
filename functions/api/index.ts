import { Env } from '../types';

// Declare Cloudflare Worker types for local linting
type R2Bucket = any;
type KVNamespace = any;
type PagesFunction<T> = any;

const buildEmptySearchResult = (keyword: string, page: number, limit: number) => ({
    code: 0,
    data: {
        keyword,
        page,
        limit,
        total: 0,
        results: []
    }
});

type SearchHandlerResult = { results: any[]; total?: number; tag?: string };
type SearchHandler = (keyword: string, limit: number, page: number) => Promise<SearchHandlerResult>;

const TUNEHUB_API_BASE = 'https://tunehub.sayqz.com/api';
const TUNEHUB_API_KEY = 'th_d83c68b0945ec6126c4413907c1c772b7bb7de08585b47f5';
const TUNEHUB_METHOD_CACHE_TTL = 5 * 60 * 1000; // 5 minutes

const SEARCH_API_BASES = [
    { base: TUNEHUB_API_BASE, tag: 'tunehub' }
];

type TunehubMethodConfig = {
    url: string;
    method: string;
    headers?: Record<string, string>;
    params?: Record<string, any>;
    body?: any;
    transform?: string;
};

const tunehubMethodCache = new Map<string, { expires: number; config: TunehubMethodConfig }>();

const SEARCH_CACHE_TTL_MS = 30 * 24 * 60 * 60 * 1000; // 30 days

const normalizeSearchKeyword = (keyword: string) => keyword.trim().toLowerCase();

const buildSearchCacheKey = (source: string, keyword: string, page: number, limit: number) => {
    const safeKeyword = encodeURIComponent(keyword);
    return `search/${source}/${safeKeyword}/p${page}-l${limit}.json`;
};

type CachedSearchPayload = {
    keyword: string;
    page: number;
    limit: number;
    total: number;
    results: any[];
};

const readSearchCache = async (bucket: R2Bucket | undefined, key: string): Promise<CachedSearchPayload | null> => {
    if (!bucket) return null;
    try {
        const object = await bucket.get(key);
        if (!object) return null;
        const text = await object.text();
        const record = JSON.parse(text);
        if (typeof record.cachedAt !== 'number' || !record.payload) {
            await bucket.delete(key);
            return null;
        }
        if (Date.now() - record.cachedAt > SEARCH_CACHE_TTL_MS) {
            await bucket.delete(key);
            return null;
        }
        return record.payload as CachedSearchPayload;
    } catch (err) {
        console.error('Search cache read error:', err);
        return null;
    }
};

const writeSearchCache = async (bucket: R2Bucket | undefined, key: string, payload: CachedSearchPayload) => {
    if (!bucket) return;
    try {
        await bucket.put(key, JSON.stringify({
            cachedAt: Date.now(),
            payload
        }), {
            httpMetadata: {
                contentType: 'application/json'
            }
        });
    } catch (err) {
        console.error('Search cache write error:', err);
    }
};

const SONG_CACHE_TTL_MS = 180 * 24 * 60 * 60 * 1000; // 180 days

const normalizeSongCacheId = (source: string, id: string) => {
    const trimmed = (id || '').trim();
    if (!trimmed) return '';
    if (source === 'kuwo') {
        return trimmed.replace(/^MUSIC_/i, '');
    }
    return trimmed;
};

const buildSongCacheKey = (source: string, id: string, quality: string) => {
    const normalizedId = normalizeSongCacheId(source, id);
    const safeId = encodeURIComponent(normalizedId || id);
    return `songs/meta/${source}/${safeId}:${quality}.json`;
};

const buildSongAudioKey = (source: string, id: string, quality: string) => {
    const normalizedId = normalizeSongCacheId(source, id);
    const safeId = encodeURIComponent(normalizedId || id);
    return `songs/audio/${source}/${safeId}:${quality}`;
};

const buildSongStreamPath = (source: string, id: string, quality: string) => {
    const normalizedId = normalizeSongCacheId(source, id);
    const safeId = encodeURIComponent(normalizedId || id);
    return `/api/?source=${source}&type=song-file&id=${safeId}&quality=${quality}`;
};

const buildSongLyricsKey = (source: string, id: string) => {
    const normalizedId = normalizeSongCacheId(source, id);
    const safeId = encodeURIComponent(normalizedId || id);
    return `songs/lyrics/${source}/${safeId}.lrc`;
};

type SongCachePayload = {
    id: string;
    source: string;
    quality: string;
    url: string;
    name?: string;
    artist?: string;
    album?: string;
    pic?: string;
    lrc?: string;
    sourceUrl?: string;
    hasFile?: boolean;
};

const readSongCache = async (bucket: R2Bucket | undefined, key: string): Promise<SongCachePayload | null> => {
    if (!bucket) return null;
    try {
        const object = await bucket.get(key);
        if (!object) return null;
        const text = await object.text();
        const record = JSON.parse(text);
        if (typeof record.cachedAt !== 'number' || !record.payload) {
            await bucket.delete(key);
            return null;
        }
        if (Date.now() - record.cachedAt > SONG_CACHE_TTL_MS) {
            await bucket.delete(key);
            return null;
        }
        return record.payload as SongCachePayload;
    } catch (err) {
        console.error('Song cache read error:', err);
        return null;
    }
};

const writeSongCache = async (bucket: R2Bucket | undefined, key: string, payload: SongCachePayload) => {
    if (!bucket) return;
    try {
        await bucket.put(key, JSON.stringify({
            cachedAt: Date.now(),
            payload
        }), {
            httpMetadata: {
                contentType: 'application/json'
            }
        });
    } catch (err) {
        console.error('Song cache write error:', err);
    }
};

const PARSE_BASES = [
    `${TUNEHUB_API_BASE}/v1/parse`
];

const PARSE_TIMEOUT_MS = 6000;

const runTunehubParse = async (platform: string, ids: string[], quality: string) => {
    const errors: string[] = [];
    for (const url of PARSE_BASES) {
        try {
            const controller = new AbortController();
            const timeout = setTimeout(() => controller.abort(), PARSE_TIMEOUT_MS);

            const resp = await fetch(url, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-API-Key': TUNEHUB_API_KEY,
                    'Authorization': `Bearer ${TUNEHUB_API_KEY}`
                },
                body: JSON.stringify({
                    platform,
                    ids: ids.join(','),
                    quality
                }),
                signal: controller.signal
            });

            clearTimeout(timeout);

            const text = await resp.text();
            if (!resp.ok) {
                errors.push(`${url} ${resp.status}: ${text.substring(0, 200)}`);
                continue;
            }
            const payload = parseJsonWithRecovery(text);
            const list = payload?.data?.data ?? payload?.data;
            return Array.isArray(list) ? list : [];
        } catch (err: any) {
            errors.push(`${url}: ${err?.message || err}`);
            console.error('TuneHub parse error:', err?.message || err);
        }
    }
    throw new Error(`TuneHub parse failed: ${errors.join(' | ')}`);
};

const buildKuwoIdVariants = (id: string) => {
    const trimmed = id.trim();
    const variants = new Set<string>();
    if (trimmed) {
        variants.add(trimmed);
        if (trimmed.startsWith('MUSIC_')) {
            variants.add(trimmed.replace(/^MUSIC_/, ''));
        } else {
            variants.add(`MUSIC_${trimmed}`);
        }
    }
    return Array.from(variants).filter(Boolean);
};

const parseRangeHeader = (header: string | null) => {
    if (!header) return null;
    const match = header.match(/bytes=(\d+)-(\d*)/i);
    if (!match) return null;
    const start = Number(match[1]);
    if (!Number.isFinite(start) || start < 0) return null;
    const endStr = match[2];
    if (!endStr) {
        return { offset: start };
    }
    const end = Number(endStr);
    if (!Number.isFinite(end) || end < start) return null;
    return { offset: start, length: end - start + 1 };
};

const loadSongMetadata = async (
    bucket: R2Bucket | undefined,
    cacheKey: string,
    source: string,
    id: string,
    quality: string,
    streamPath: string
): Promise<SongCachePayload | null> => {
    const qualityCandidates = Array.from(new Set([quality, '128k']));
    const attemptIds = source === 'kuwo' ? buildKuwoIdVariants(id) : [id];
    for (const candidate of attemptIds) {
        if (!candidate) continue;
        for (const q of qualityCandidates) {
            try {
                const parseResults = await runTunehubParse(source, [candidate], q);
                const matched = parseResults.find((item: any) => item && item.success !== false && item.url);
                if (matched && matched.url) {
                    const payload: SongCachePayload = {
                        id: normalizeSongCacheId(source, matched.id || id) || id,
                        source,
                        quality: q,
                        url: streamPath,
                        name: matched.info?.name || '',
                        artist: matched.info?.artist || '',
                        album: matched.info?.album || '',
                        pic: sanitizeCoverUrl(matched.cover || ''),
                        lrc: matched.lyrics || '',
                        sourceUrl: matched.url,
                        hasFile: false
                    };
                    await writeSongCache(bucket, cacheKey, payload);
                    return payload;
                }
                console.warn(`Parse returned empty for source=${source} id=${candidate} quality=${q}`);
            } catch (err) {
                console.error('TuneHub metadata load failed:', err);
            }
        }
    }
    return null;
};

const sanitizeSongPayload = (payload: SongCachePayload): SongCachePayload => {
    const { sourceUrl, ...safe } = payload;
    return { ...safe };
};

const readSongLyrics = async (bucket: R2Bucket | undefined, key: string): Promise<string | null> => {
    if (!bucket) return null;
    try {
        const object = await bucket.get(key);
        if (!object) return null;
        return await object.text();
    } catch (err) {
        console.error('Song lyrics read error:', err);
        return null;
    }
};

const isLyricText = (value: string) => {
    if (!value) return false;
    const trimmed = value.trim();
    if (!trimmed) return false;
    if (trimmed.startsWith('[') || trimmed.includes('\n')) return true;
    return false;
};

const fetchLyricFromUrl = async (source: string, url: string) => {
    const headers = new Headers(LYRICS_HEADERS[source] || {});
    const resp = await fetch(url, { headers });
    if (!resp.ok) {
        const text = await resp.text();
        throw new Error(`Lyric fetch HTTP ${resp.status}: ${text.substring(0, 200)}`);
    }
    return resp.text();
};

const resolveLyricText = async (source: string, lyricSource?: string) => {
    if (!lyricSource) return '';
    const trimmed = lyricSource.trim();
    if (!trimmed) return '';
    if (trimmed.startsWith('data:text/plain')) {
        try {
            const [, meta, data] = trimmed.match(/^data:text\/plain(?:;charset=[^,]+)?;(base64)?,(.*)$/i) || [];
            if (meta === 'base64' && data) {
                return atob(data);
            }
        } catch {
            // ignore
        }
    }
    if (isLyricText(trimmed) && !/^https?:\/\//i.test(trimmed.split(/\s+/)[0])) {
        return trimmed;
    }
    if (/^https?:\/\//i.test(trimmed)) {
        try {
            return await fetchLyricFromUrl(source, trimmed);
        } catch (err) {
            console.error('Lyric fetch failed:', err);
            return '';
        }
    }
    return trimmed;
};

const writeSongLyrics = async (bucket: R2Bucket | undefined, key: string, lyrics: string) => {
    if (!bucket) return;
    try {
        await bucket.put(key, lyrics, {
            httpMetadata: {
                contentType: 'text/plain; charset=utf-8'
            }
        });
    } catch (err) {
        console.error('Song lyrics write error:', err);
    }
};

const getTunehubMethodConfig = async (platform: string, func: string): Promise<TunehubMethodConfig> => {
    const cacheKey = `${platform}:${func}`;
    const now = Date.now();
    const cached = tunehubMethodCache.get(cacheKey);
    if (cached && cached.expires > now) {
        return cached.config;
    }

    const resp = await fetch(`${TUNEHUB_API_BASE}/v1/methods/${platform}/${func}`, {
        headers: {
            'X-API-Key': TUNEHUB_API_KEY,
            'Authorization': `Bearer ${TUNEHUB_API_KEY}`
        }
    });

    if (!resp.ok) {
        const message = await resp.text();
        throw new Error(`TuneHub method ${platform}/${func} HTTP ${resp.status}: ${message}`);
    }

    const payload = await resp.json();
    if (payload.code !== 0 || !payload.data) {
        throw new Error(`TuneHub method ${platform}/${func} invalid response`);
    }

    const config = payload.data as TunehubMethodConfig;
    tunehubMethodCache.set(cacheKey, { config, expires: now + TUNEHUB_METHOD_CACHE_TTL });
    return config;
};

const evaluateTemplateExpression = (expression: string, variables: Record<string, any>) => {
    const expr = expression.trim();

    const toNumber = (val: any, fallback: number) => {
        const num = Number(val);
        return Number.isFinite(num) ? num : fallback;
    };

    const page = toNumber(variables.page, 1);
    const limit = toNumber(variables.limit ?? variables.pageSize, toNumber(variables.pageSize, 20));

    const keyword = variables.keyword ?? '';

    // Common patterns used by TuneHub templates
    if (expr === 'keyword') return keyword;
    if (expr === 'page' || expr === 'page || 1' || expr === '(page || 1)') return page;
    if (expr === 'limit' || expr === 'limit || 20' || expr === '(limit || 20)') return limit;
    if (expr === 'pageSize' || expr === 'pageSize || limit || 20' || expr === '(pageSize || limit || 20)') {
        return toNumber(variables.pageSize, limit);
    }
    if (expr === '(page || 1) - 1' || expr === '((page || 1) - 1)') {
        return page - 1;
    }
    if (expr === '((page || 1) - 1) * (limit || 20)' || expr === '(page - 1) * limit') {
        return (page - 1) * limit;
    }

    // Simple fallback pattern: var || default
    const fallbackMatch = expr.match(/^(\w+)\s*\|\|\s*(.+)$/);
    if (fallbackMatch) {
        const [, name, defRaw] = fallbackMatch;
        const val = variables[name];
        if (val !== undefined && val !== null && val !== '') return val;
        const defNum = Number(defRaw);
        if (Number.isFinite(defNum)) return defNum;
        return defRaw.replace(/^['"]|['"]$/g, '');
    }

    // parseInt(var)
    const parseIntMatch = expr.match(/^parseInt\((.+)\)$/i);
    if (parseIntMatch) {
        const inner = evaluateTemplateExpression(parseIntMatch[1], variables);
        const num = parseInt(String(inner), 10);
        return Number.isFinite(num) ? num : inner;
    }

    return variables[expr] ?? '';
};

const resolveTemplateValue = (value: any, variables: Record<string, any>) => {
    if (typeof value !== 'string') return value;
    if (!value.includes('{{')) return value;

    const fullMatch = value.trim().match(/^\{\{\s*(.+?)\s*\}\}$/);
    if (fullMatch) {
        return evaluateTemplateExpression(fullMatch[1], variables);
    }

    return value.replace(/\{\{\s*(.+?)\s*\}\}/g, (_, expr) => {
        const result = evaluateTemplateExpression(expr, variables);
        return result === undefined || result === null ? '' : String(result);
    });
};

const applyTemplates = (input: any, variables: Record<string, any>): any => {
    if (Array.isArray(input)) {
        return input.map(item => applyTemplates(item, variables));
    }
    if (input && typeof input === 'object') {
        const output: Record<string, any> = {};
        for (const [key, value] of Object.entries(input)) {
            output[key] = applyTemplates(value, variables);
        }
        return output;
    }
    return resolveTemplateValue(input, variables);
};

const applyTunehubTransform = (data: any, transform?: string) => {
    if (transform) {
        console.warn('Skipping TuneHub transform execution (disabled in Workers).');
    }
    return data;
};

const parseJsonWithRecovery = (text: string) => {
    if (!text) return {};
    try {
        return JSON.parse(text);
    } catch {
        const braceIndex = text.indexOf('{');
        const bracketIndex = text.indexOf('[');
        const candidates = [braceIndex, bracketIndex].filter(idx => idx >= 0);
        if (candidates.length > 0) {
            const start = Math.min(...candidates);
            try {
                return JSON.parse(text.substring(start));
            } catch {
                // ignore
            }
        }
        throw new Error('Invalid JSON response');
    }
};

const invokeTunehubMethod = async (config: TunehubMethodConfig, variables: Record<string, any>) => {
    if (!config.url) {
        throw new Error('TuneHub method missing URL');
    }

    const resolvedUrl = resolveTemplateValue(config.url, variables);
    const params = config.params ? applyTemplates(config.params, variables) : undefined;
    const headersData = config.headers ? applyTemplates(config.headers, variables) : undefined;

    let finalUrl = String(resolvedUrl);
    if (params && typeof params === 'object') {
        const searchParams = new URLSearchParams();
        for (const [key, value] of Object.entries(params)) {
            if (value === undefined || value === null || value === '') continue;
            searchParams.append(key, String(value));
        }
        const query = searchParams.toString();
        if (query) {
            finalUrl += (finalUrl.includes('?') ? '&' : '?') + query;
        }
    }

    const headers = new Headers();
    if (headersData && typeof headersData === 'object') {
        for (const [key, value] of Object.entries(headersData)) {
            headers.set(key, String(value));
        }
    }
    // propagate key for upstream services that require it
    if (!headers.has('X-API-Key')) {
        headers.set('X-API-Key', TUNEHUB_API_KEY);
    }
    if (!headers.has('Authorization')) {
        headers.set('Authorization', `Bearer ${TUNEHUB_API_KEY}`);
    }

    const method = (config.method || 'GET').toUpperCase();
    const init: RequestInit = { method, headers };

    if (method === 'POST' || method === 'PUT' || method === 'PATCH') {
        const bodyData = config.body ? applyTemplates(config.body, variables) : undefined;
        if (bodyData !== undefined) {
            if (typeof bodyData === 'string') {
                init.body = bodyData;
            } else {
                init.body = JSON.stringify(bodyData);
                if (!headers.has('Content-Type')) {
                    headers.set('Content-Type', 'application/json');
                }
            }
        }
    }

    // QQ Music specific overrides - ensure mobile protocol
    if (finalUrl.includes('y.qq.com') && init.body && typeof init.body === 'string') {
        try {
            const body = JSON.parse(init.body);
            if (body.comm) {
                body.comm.ct = 11;
                body.comm.cv = 0;
                init.body = JSON.stringify(body);
            }
        } catch (e) {
            // ignore parse errors
        }
    }

    const resp = await fetch(finalUrl, init);
    const text = await resp.text();
    if (!resp.ok) {
        throw new Error(`TuneHub upstream ${resp.status}: ${text.substring(0, 200)}`);
    }
    return parseJsonWithRecovery(text);
};

const extractSearchList = (data: any): any[] => {
    if (!data) return [];
    if (Array.isArray(data)) return data;
    if (Array.isArray(data.results)) return data.results;
    if (Array.isArray(data.list)) return data.list;
    if (Array.isArray(data.songs)) return data.songs;
    if (Array.isArray((data as any).abslist)) return (data as any).abslist; // Kuwo search
    if (data.data) {
        if (Array.isArray(data.data)) return data.data;
        if (Array.isArray(data.data.results)) return data.data.results;
        if (Array.isArray(data.data.list)) return data.data.list;
        if (Array.isArray(data.data.songs)) return data.data.songs;
        if (Array.isArray(data.data.abslist)) return data.data.abslist;
        // QQ nested path
        if (data.data.body?.song?.list) return data.data.body.song.list;
        if (data.data.body?.song?.songlist) return data.data.body.song.songlist;
    }
    if (data.result && Array.isArray(data.result.songs)) return data.result.songs;

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
 * Handles playlist data structures that differ from search results:
 * - Netease: playlist.tracks, result.tracks
 * - Kuwo: musiclist, data.musiclist
 * - QQ: various nested paths (songlist, songInfoList, etc.)
 * Falls back to extractSearchList for generic structures.
 */
const extractPlaylistTracks = (data: any, platform: string): any[] => {
    if (!data) return [];

    // Netease: playlist.tracks is the primary path
    if (platform === 'netease') {
        const candidates = [
            data.playlist?.tracks,
            data.data?.playlist?.tracks,
            data.result?.playlist?.tracks,
            data.result?.tracks,
            data.tracks,
            data.playlist?.trackIds, // sometimes only IDs are returned
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
    if (platform === 'qq' || platform === 'tencent') {
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
    return extractSearchList(data);
};

const formatArtist = (value: any): string => {
    if (!value) return '';
    if (typeof value === 'string') {
        return value.replace(/&/g, ', ');
    }
    if (Array.isArray(value)) {
        return value.map(item => formatArtist(item)).filter(Boolean).join(', ');
    }
    if (typeof value === 'object') {
        if (typeof value.name === 'string') return value.name;
        if (typeof value.artist === 'string') return value.artist;
        if (typeof value.artistName === 'string') return value.artistName;
        if (Array.isArray((value as any).list)) {
            return formatArtist((value as any).list);
        }
    }
    return '';
};

const buildQQAlbumPic = (albumMid?: string) => {
    if (!albumMid) return '';
    return `https://y.gtimg.cn/music/photo_new/T002R300x300M000${albumMid}.jpg`;
};

const sanitizeCoverUrl = (input?: string) => {
    if (typeof input !== 'string') return '';
    if (input.startsWith('http://') && !/\.music\.126\.net/i.test(input)) {
        return input.replace(/^http:\/\//i, 'https://');
    }
    // Kuwo often returns relative paths like "120/xx/yy/zz.jpg"
    if (/^\d{2,3}\//.test(input)) {
        return `https://img4.kuwo.cn/star/albumcover/${input.replace(/^\d{2,3}\//, '')}`;
    }
    return input;
};

const resolveAlbumPic = (item: any) =>
    item.pic ||
    item.cover ||
    item.picUrl ||
    item.coverUrl ||
    item.frontPic ||
    item.frontPicUrl ||
    item.headPic ||
    item.headPicUrl ||
    item.coverImg ||
    item.coverImgUrl ||
    item.albumpic ||
    item.albumpic_big ||
    item.albumpic_small ||
    item.albumpic120 ||
    item.albumpic250 ||
    item.albumPic ||
    item.album_pic ||
    item.albumPicUrl ||
    item.album_cover ||
    item.album?.picUrl ||
    item.album?.pic ||
    item.album?.cover ||
    item.album?.img ||
    item.album?.imgUrl ||
    item.album?.pic120 ||
    item.web_albumpic ||
    item.web_albumpic_short ||
    item.hts_MVPIC ||
    item.MVPIC ||
    item.web_artistpic_short ||
    item.web_albumpic_short ||
    item.pic120 ||
    item.pic160 ||
    item.pic240 ||
    item.pic500 ||
    item.pic640 ||
    (item.album?.mid ? buildQQAlbumPic(item.album.mid) : '') ||
    (item.albumMid ? buildQQAlbumPic(item.albumMid) : '') ||
    (item.albumpic_big ? item.albumpic_big : '');

const normalizeSongItem = (item: any, platform: string) => {
    const idValue =
        item.mid ??
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
    const id = typeof idValue === 'string' ? idValue.replace(/^MUSIC_/, '') : idValue;
    const name = item.name ?? item.songname ?? item.songName ?? item.title ?? item.SONGNAME ?? '';
    const artistSource = item.artist ?? item.artists ?? item.singer ?? item.ARTIST ?? item.artistname ?? item.artistName ?? item.author;
    const albumSource = item.album?.name ?? item.albumName ?? item.albumname ?? item.ALBUM ?? item.album ?? '';
    const picSource = resolveAlbumPic(item);

    return {
        id: id ? String(id) : '',
        name: typeof name === 'string' ? name : '',
        artist: formatArtist(artistSource) ||
            formatArtist(item.artistinfo) ||
            (Array.isArray(item.singer) ? item.singer.map((s: any) => s?.name || '').filter(Boolean).join('/') : ''),
        album: typeof albumSource === 'string' ? albumSource : (item.album?.title || ''),
        pic: typeof picSource === 'string' ? sanitizeCoverUrl(picSource) : '',
        platform: item.platform || platform
    };
};

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
        if (nested.meta && typeof nested.meta.sum === 'number') return nested.meta.sum;
    }
    const root = data.req || data.search || data;
    return root?.data?.body?.song?.totalnum ??
        root?.data?.body?.song?.total_num ??
        root?.data?.meta?.sum ??
        fallback;
};

const searchViaTunehubApi = async (
    platform: string,
    keyword: string,
    limit: number,
    page: number,
    normalizedPlatform?: string
): Promise<SearchHandlerResult> => {
    const safePage = Number.isFinite(page) && page > 0 ? page : 1;
    const safeLimit = Number.isFinite(limit) && limit > 0 ? limit : 20;
    const commonParams = {
        source: platform,
        platform,
        keyword,
        limit: String(safeLimit),
        page: String(safePage),
        type: 'search'
    };

    const errors: string[] = [];
    let fallbackResult: SearchHandlerResult | null = null;

    for (const { base, tag } of SEARCH_API_BASES) {
        const attempts = [
            { path: `${base}/v1/search`, params: commonParams },
            { path: `${base}/search`, params: commonParams },
            { path: `${base}/`, params: { ...commonParams, type: 'search' } }
        ];

        for (const attempt of attempts) {
            const searchParams = new URLSearchParams();
            Object.entries(attempt.params).forEach(([k, v]) => {
                if (v !== undefined && v !== null && v !== '') searchParams.append(k, String(v));
            });
            const url = attempt.path + (attempt.path.includes('?') ? '&' : '?') + searchParams.toString();

            const headers: Record<string, string> = {
                'X-API-Key': TUNEHUB_API_KEY,
                'Authorization': `Bearer ${TUNEHUB_API_KEY}`
            };

            // Try GET first
            try {
                const resp = await fetch(url, { headers });

                const text = await resp.text();
                if (!resp.ok) {
                    errors.push(`${tag} ${resp.status} ${attempt.path}: ${text.substring(0, 200)}`);
                } else {
                    const payload = parseJsonWithRecovery(text);
                    const data = payload?.data ?? payload;
                    const list = extractSearchList(data);
                    const normalized = Array.isArray(list)
                        ? list
                            .map((item: any) => normalizeSongItem(item, normalizedPlatform || platform))
                            .filter((item: any) => item.id && item.name)
                        : [];
                    const total = determineTotal(data, normalized.length);
                    const result: SearchHandlerResult = { results: normalized, total, tag: `api:${tag}` };

                    if (normalized.length > 0) {
                        return result;
                    }
                    if (!fallbackResult) {
                        fallbackResult = result;
                    }
                }
            } catch (err: any) {
                const message = err?.message || String(err);
                errors.push(`${tag} ${attempt.path}: ${message}`);
                console.error(`Search API error (${tag} GET):`, message);
            }

            // Also try POST for /v1/search if GET failed and this attempt is /v1/search
            if (attempt.path.endsWith('/v1/search')) {
                try {
                    const resp = await fetch(attempt.path, {
                        method: 'POST',
                        headers: {
                            ...headers,
                            'Content-Type': 'application/json'
                        },
                        body: JSON.stringify({
                            keyword,
                            source: platform,
                            platform,
                            limit: safeLimit,
                            page: safePage
                        })
                    });
                    const text = await resp.text();
                    if (!resp.ok) {
                        errors.push(`${tag} POST ${resp.status} ${attempt.path}: ${text.substring(0, 200)}`);
                        continue;
                    }
                    const payload = parseJsonWithRecovery(text);
                    const data = payload?.data ?? payload;
                    const list = extractSearchList(data);
                    const normalized = Array.isArray(list)
                        ? list
                            .map((item: any) => normalizeSongItem(item, normalizedPlatform || platform))
                            .filter((item: any) => item.id && item.name)
                        : [];
                    const total = determineTotal(data, normalized.length);
                    const result: SearchHandlerResult = { results: normalized, total, tag: `api:${tag}-post` };
                    if (normalized.length > 0) {
                        return result;
                    }
                    if (!fallbackResult) {
                        fallbackResult = result;
                    }
                } catch (err: any) {
                    const message = err?.message || String(err);
                    errors.push(`${tag} POST ${attempt.path}: ${message}`);
                    console.error(`Search API error (${tag} POST):`, message);
                }
            }
        }
    }

    if (fallbackResult) {
        return fallbackResult;
    }
    if (errors.length) {
        throw new Error(errors.join(' | '));
    }
    return { results: [], total: 0 };
};

const fetchToplistsViaTunehubApi = async (platform: string, normalizedPlatform?: string) => {
    try {
        const resp = await fetch(`${TUNEHUB_API_BASE}/v1/methods/${platform}/toplists`, {
            headers: { 'X-API-Key': TUNEHUB_API_KEY }
        });
        if (!resp.ok) {
            const msg = await resp.text();
            throw new Error(`Toplists methods ${resp.status}: ${msg.substring(0, 200)}`);
        }
        const configPayload = await resp.json();
        const config = configPayload?.data;
        if (!config?.url) throw new Error('Toplists methods missing url');

        const apiResp = await invokeTunehubMethod(config, {});

        if (platform === 'kuwo') {
            const child = apiResp.child || apiResp.data?.child || [];
            const list = Array.isArray(child)
                ? child
                    .filter((item: any) => item.source === '1' || item.source === 1 || !item.source)
                    .map((item: any) => ({
                        id: item.sourceid || item.id,
                        name: item.name,
                        pic: sanitizeCoverUrl(item.pic || ''),
                        updateFrequency: item.info || '',
                        desc: ''
                    }))
                : [];
            return list.map(t => normalizeToplistItem(t, normalizedPlatform || platform));
        }

        if (platform === 'qq') {
            const list: any[] = [];

            const pushGroup = (groups?: any[]) => {
                if (!Array.isArray(groups)) return;
                groups.forEach((g: any) => {
                    if (Array.isArray(g?.toplist)) {
                        g.toplist.forEach((t: any) => list.push(t));
                    }
                });
            };

            pushGroup(apiResp.req_0?.data?.group);
            pushGroup(apiResp.req?.data?.group);
            pushGroup(apiResp.list?.data?.group);
            pushGroup(apiResp.data?.group);
            pushGroup(apiResp.group);

            if (!list.length && Array.isArray(apiResp.req_0?.data?.toplist)) {
                list.push(...apiResp.req_0.data.toplist);
            }
            if (!list.length && Array.isArray(apiResp.data?.list)) {
                list.push(...apiResp.data.list);
            }
            if (!list.length && Array.isArray(apiResp.toplist || apiResp.topList)) {
                list.push(...(apiResp.toplist || apiResp.topList));
            }
            if (!list.length) {
                const flat = flattenToplistEntries(apiResp);
                flat.forEach((t: any) => list.push(t));
            }

            return list
                .map((t: any) => normalizeToplistItem(t, normalizedPlatform || platform))
                .filter(item => item.id && item.name);
        }

        if (platform === 'netease') {
            const candidates: any[] = [];
            if (Array.isArray(apiResp.list)) candidates.push(...apiResp.list);
            if (Array.isArray(apiResp.data?.list)) candidates.push(...apiResp.data.list);
            if (Array.isArray(apiResp.result?.list)) candidates.push(...apiResp.result.list);
            if (Array.isArray(apiResp.playlist?.list)) candidates.push(...apiResp.playlist.list);
            if (!candidates.length) {
                const flat = flattenToplistEntries(apiResp);
                candidates.push(...flat);
            }
            return candidates
                .map((t: any) => normalizeToplistItem(t, normalizedPlatform || platform))
                .filter(item => item.id && item.name);
        }

        const list = apiResp.list || apiResp.data?.list || apiResp.results || flattenToplistEntries(apiResp);
        return Array.isArray(list) ? list.map((t: any) => normalizeToplistItem(t, normalizedPlatform || platform)) : [];
    } catch (err) {
        console.error(`${platform} toplists methods failed:`, err?.message || err);
        return [];
    }
};

const fetchToplistSongsViaTunehubApi = async (platform: string, id: string, normalizedPlatform?: string) => {
    try {
        const resp = await fetch(`${TUNEHUB_API_BASE}/v1/methods/${platform}/toplist`, {
            headers: { 'X-API-Key': TUNEHUB_API_KEY }
        });
        if (!resp.ok) {
            const msg = await resp.text();
            throw new Error(`Toplist methods ${resp.status}: ${msg.substring(0, 200)}`);
        }
        const configPayload = await resp.json();
        const config = configPayload?.data;
        if (!config?.url) throw new Error('Toplist methods missing url');

        const apiResp = await invokeTunehubMethod(config, { id, topId: id, topid: id });

        if (platform === 'kuwo') {
            const songList = apiResp.musiclist || apiResp.data?.musiclist || apiResp.data || [];
            return Array.isArray(songList) ? songList.map((s: any) => normalizeSongItem(s, normalizedPlatform || platform)) : [];
        }

        if (platform === 'netease') {
            const songList = apiResp?.playlist?.tracks ||
                apiResp.tracks ||
                apiResp.data?.playlist?.tracks ||
                apiResp.result?.tracks ||
                extractSearchList(apiResp);
            return Array.isArray(songList) ? songList.map((s: any) => normalizeSongItem(s, normalizedPlatform || platform)) : [];
        }

        if (platform === 'qq') {
            const candidates = [
                apiResp.req_0?.data?.songInfoList,
                apiResp.req?.data?.songInfoList,
                apiResp.detail?.data?.songInfoList,
                apiResp.data?.songInfoList,
                apiResp.songInfoList,
                apiResp.toplist?.data?.songInfoList,
                apiResp.toplist?.songInfoList,
                apiResp.req_0?.data?.song,
                apiResp.req?.data?.song,
                apiResp.data?.song,
                apiResp.song,
                apiResp.req_0?.data?.data?.song,
                apiResp.req?.data?.data?.song,
                apiResp.data?.data?.song,
                apiResp.req_0?.data?.track_info?.list,
                apiResp.req?.data?.track_info?.list,
                apiResp.data?.track_info?.list,
                apiResp.req_0?.data?.data?.track_info?.list,
                apiResp.req?.data?.data?.track_info?.list,
                apiResp.data?.data?.track_info?.list,
                apiResp.req_0?.data?.songlist,
                apiResp.req?.data?.songlist,
                apiResp.data?.songlist,
                apiResp.songlist,
                apiResp.req_0?.data?.songList,
                apiResp.req?.data?.songList,
                apiResp.data?.songList,
                apiResp.songList
            ];

            let songList: any[] = [];
            for (const cand of candidates) {
                if (Array.isArray(cand) && cand.length) {
                    songList = cand;
                    break;
                }
            }

            if (!songList.length) {
                songList = extractSearchList(apiResp);
            }

            return Array.isArray(songList) ? songList.map((s: any) => normalizeSongItem(s, normalizedPlatform || platform)) : [];
        }

        const songList = extractSearchList(apiResp);
        return Array.isArray(songList) ? songList.map(s => normalizeSongItem(s, normalizedPlatform || platform)) : [];
    } catch (err) {
        console.error(`${platform} toplist methods failed:`, err?.message || err);
        return [];
    }
};

const searchViaTunehubMethod = async (
    platform: string,
    keyword: string,
    limit: number,
    page: number,
    normalizedPlatform?: string
): Promise<SearchHandlerResult> => {
    const config = await getTunehubMethodConfig(platform, 'search');
    const safePage = Number.isFinite(page) && page > 0 ? page : 1;
    const safeLimit = Number.isFinite(limit) && limit > 0 ? limit : 20;
    const variables: Record<string, any> = {
        keyword,
        page: safePage,
        limit: safeLimit,
        pageSize: safeLimit,
        offset: (safePage - 1) * safeLimit
    };

    if (platform === 'tencent' || platform === 'qq') {
        variables.search_type = 0;
        variables.ct = 11;
        variables.cv = 0;
    }

    const rawData = await invokeTunehubMethod(config, variables);
    const transformed = applyTunehubTransform(rawData, config.transform);
    const list = extractSearchList(transformed);
    const normalized = list
        .map(item => normalizeSongItem(item, normalizedPlatform || platform))
        .filter(item => item.id && item.name);
    const total = determineTotal(transformed, normalized.length);

    if (normalized.length === 0) {
        try {
            const sample = JSON.stringify(transformed).slice(0, 800);
            console.warn(`TuneHub method returned empty list for ${platform}. transform=${Boolean(config.transform)} sample=${sample}`);
        } catch {
            console.warn(`TuneHub method returned empty list for ${platform} and sample stringify failed.`);
        }
    }

    return { results: normalized, total, tag: 'tunehub-method' };
};

const fetchTunehubMethodData = async (platform: string, func: string, variables: Record<string, any> = {}) => {
    const config = await getTunehubMethodConfig(platform, func);
    const rawData = await invokeTunehubMethod(config, variables);
    return applyTunehubTransform(rawData, config.transform);
};

const flattenToplistEntries = (data: any): any[] => {
    if (!data) return [];
    const keysToCheck = [
        'list', 'lists', 'data', 'menu', 'menuList', 'items', 'results',
        'group', 'groups', 'toplist', 'topList', 'bangList', 'banglist', 'bang_menu',
        'children', 'menus'
    ];
    const results: any[] = [];
    const visited = new WeakSet<object>();

    const looksLikeToplist = (obj: any) =>
        obj && typeof obj === 'object' && (
            obj.id || obj.topId || obj.topid || obj.listId || obj.listid || obj.toplistId || obj.toplistid || obj.name || obj.title
        );

    const traverse = (input: any) => {
        if (!input || typeof input !== 'object') return;
        if (visited.has(input as object)) return;
        visited.add(input as object);

        if (Array.isArray(input)) {
            input.forEach(entry => traverse(entry));
            return;
        }

        let hitArray = false;
        for (const key of keysToCheck) {
            const direct = (input as any)[key];
            if (Array.isArray(direct)) {
                direct.forEach(entry => traverse(entry));
                hitArray = true;
            }
            const nested = (input as any).data && typeof (input as any).data === 'object' ? (input as any).data[key] : undefined;
            if (Array.isArray(nested)) {
                nested.forEach(entry => traverse(entry));
                hitArray = true;
            }
        }

        Object.values(input as any).forEach(val => {
            if (val && typeof val === 'object') {
                traverse(val);
            }
        });

        if (!hitArray && looksLikeToplist(input)) {
            results.push(input);
        }
    };

    traverse(data);
    return results;
};

const normalizeToplistItem = (item: any, platform: string) => {
    const id =
        item.id ??
        item.topId ??
        item.topid ??
        item.sourceid ??
        item.sourceId ??
        item.listid ??
        item.listId ??
        item.toplistId ??
        item.toplistid ??
        item.bangId ??
        item.bangid;

    const name = item.name ?? item.title ?? item.topTitle ?? item.listName ?? item.bangName ?? item.menuName;
    const pic = item.pic || item.picUrl || item.cover || item.coverImgUrl || item.frontPicUrl || item.headPic || item.logo || item.img || item.imgurl || item.image || '';
    const updateFrequency = item.updateFrequency ?? item.pub ?? item.pub_time ?? item.updateTime ?? item.period ?? '';
    const desc = item.desc ?? item.intro ?? item.info ?? item.description ?? '';

    return {
        id: id ? String(id) : '',
        name: typeof name === 'string' ? name : '',
        pic: typeof pic === 'string' ? sanitizeCoverUrl(pic.replace(/^http:\/\//, 'https://')) : '',
        updateFrequency: typeof updateFrequency === 'string' ? updateFrequency : '',
        desc: typeof desc === 'string' ? desc : '',
        platform
    };
};

const COMMON_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
};

const PLATFORM_REFERERS: Record<string, string> = {
    netease: 'https://music.163.com/',
    qq: 'https://y.qq.com/',
    tencent: 'https://y.qq.com/',
    kuwo: 'https://www.kuwo.cn/',
};

const LYRICS_HEADERS: Record<string, Record<string, string>> = {
    netease: {
        'Referer': 'https://music.163.com/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    },
    qq: {
        'Referer': 'https://y.qq.com/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    },
    kuwo: {
        'Referer': 'https://kuwo.cn/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
};

const handleTunehubSearch = async (env: Env, source: string, params: Record<string, string>, corsHeaders: Record<string, string>) => {
    const keyword = params.keyword ? String(params.keyword) : '';
    const limit = parseInt(params.limit || '30', 10);
    const page = parseInt(params.page || '1', 10);

    if (!keyword.trim()) {
        return new Response(JSON.stringify(buildEmptySearchResult(keyword, page, limit)), {
            headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
    }

    const normalizedKeyword = normalizeSearchKeyword(keyword);
    const cacheKey = buildSearchCacheKey(source, normalizedKeyword, page, limit);

    let searchResult: SearchHandlerResult | null = null;
    let handlerUsed: string = 'tunehub-method';
    const tunehubSource = source === 'tencent' ? 'qq' : source;

    try {
        const result = await searchViaTunehubMethod(tunehubSource, keyword, limit, page, source);
        searchResult = result;
        handlerUsed = result.tag || 'tunehub-method';
        if (result.results.length === 0) {
            console.warn('TuneHub method returned empty results.');
        }
    } catch (err: any) {
        console.error('TuneHub method search error:', err?.message || err);
    }

    const results = searchResult?.results ?? [];
    const total = searchResult?.total ?? results.length;

    console.log(`[search] source=${source} keyword="${keyword}" handler=${handlerUsed} results=${results.length}`);

    return new Response(JSON.stringify({
        code: 0,
        data: {
            keyword,
            page,
            limit,
            total,
            results
        }
    }), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
};

export const onRequest: PagesFunction<Env> = async (context) => {
    const { request } = context;
    const url = new URL(request.url);
    const params = Object.fromEntries(url.searchParams);

    // CORS Headers
    const corsHeaders = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': '*',
    };

    if (request.method === 'OPTIONS') {
        return new Response(null, { headers: corsHeaders });
    }

    const source = params.source;
    const type = params.type;

    if (type === 'relay') {
        if (request.method !== 'POST') {
            return new Response(JSON.stringify({ code: 405, error: 'Method not allowed' }), {
                status: 405,
                headers: { 'Content-Type': 'application/json', ...corsHeaders }
            });
        }
        try {
            const config = await request.json() as any;
            if (!config.url) throw new Error('Relay: missing URL');

            const headers = new Headers();
            if (config.headers) {
                for (const [k, v] of Object.entries(config.headers)) {
                    headers.set(k, String(v));
                }
            }

            // Ensure basic headers if missing
            if (!headers.has('User-Agent')) headers.set('User-Agent', COMMON_HEADERS['User-Agent']);
            if (!headers.has('Referer')) {
                const urlObj = new URL(config.url);
                if (urlObj.hostname.includes('qq.com')) headers.set('Referer', PLATFORM_REFERERS.qq);
                else if (urlObj.hostname.includes('163.com')) headers.set('Referer', PLATFORM_REFERERS.netease);
                else if (urlObj.hostname.includes('kuwo.cn')) headers.set('Referer', PLATFORM_REFERERS.kuwo);
            }

            const resp = await fetch(config.url, {
                method: config.method || 'GET',
                headers,
                body: config.body ? (typeof config.body === 'string' ? config.body : JSON.stringify(config.body)) : undefined
            });

            const responseData = await resp.text();
            return new Response(responseData, {
                status: resp.status,
                headers: {
                    'Content-Type': resp.headers.get('Content-Type') || 'application/json',
                    ...corsHeaders
                }
            });
        } catch (e: any) {
            return new Response(JSON.stringify({ code: 500, error: e.message }), {
                status: 500,
                headers: { 'Content-Type': 'application/json', ...corsHeaders }
            });
        }
    }

    if (type === 'song') {
        if (request.method !== 'GET') {
            return new Response(JSON.stringify({ code: 405, error: 'Method not allowed' }), {
                status: 405,
                headers: { 'Content-Type': 'application/json', ...corsHeaders }
            });
        }

        if (!source || (source !== 'netease' && source !== 'kuwo' && source !== 'qq' && source !== 'tencent')) {
            return new Response(JSON.stringify({ code: 400, error: 'Invalid source' }), {
                status: 400,
                headers: { 'Content-Type': 'application/json', ...corsHeaders }
            });
        }

        const resolvedSource = source === 'tencent' ? 'qq' : source;
        const id = params.id;
        if (!id) {
            return new Response(JSON.stringify({ code: 400, error: 'Missing song id' }), {
                status: 400,
                headers: { 'Content-Type': 'application/json', ...corsHeaders }
            });
        }

        const quality = params.quality || '320k';
        const cacheKey = buildSongCacheKey(resolvedSource, id, quality);
        const streamPath = buildSongStreamPath(resolvedSource, id, quality);
        let cached = await readSongCache(context.env.SEARCH_CACHE, cacheKey);

        if (!cached) {
            cached = await loadSongMetadata(context.env.SEARCH_CACHE, cacheKey, resolvedSource, id, quality, streamPath);
            if (!cached) {
                return new Response(JSON.stringify({ code: 404, error: 'Song not found' }), {
                    status: 404,
                    headers: { 'Content-Type': 'application/json', ...corsHeaders }
                });
            }
        }

        const responsePayload = sanitizeSongPayload({
            ...cached,
            url: streamPath
        });

        return new Response(JSON.stringify({
            code: 0,
            cached: Boolean(cached.hasFile),
            data: responsePayload
        }), {
            headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
    }

    if (type === 'song-file') {
        if (request.method !== 'GET' && request.method !== 'HEAD') {
            return new Response(JSON.stringify({ code: 405, error: 'Method not allowed' }), {
                status: 405,
                headers: { 'Content-Type': 'application/json', ...corsHeaders }
            });
        }

        if (!source || (source !== 'netease' && source !== 'kuwo' && source !== 'qq' && source !== 'tencent')) {
            return new Response(JSON.stringify({ code: 400, error: 'Invalid source' }), {
                status: 400,
                headers: { 'Content-Type': 'application/json', ...corsHeaders }
            });
        }

        const resolvedSource = source === 'tencent' ? 'qq' : source;
        const id = params.id;
        if (!id) {
            return new Response(JSON.stringify({ code: 400, error: 'Missing song id' }), {
                status: 400,
                headers: { 'Content-Type': 'application/json', ...corsHeaders }
            });
        }

        const quality = params.quality || '320k';
        const audioKey = buildSongAudioKey(resolvedSource, id, quality);
        const cacheKey = buildSongCacheKey(resolvedSource, id, quality);
        const streamPath = buildSongStreamPath(resolvedSource, id, quality);
        const rangeHeader = request.headers.get('Range');
        const parsedRange = parseRangeHeader(rangeHeader);

        try {
            const object = await context.env.SEARCH_CACHE.get(audioKey, parsedRange ? { range: parsedRange } : undefined);
            if (object) {
                const headers: Record<string, string> = {
                    'Content-Type': object.httpMetadata?.contentType || 'audio/mpeg',
                    'Accept-Ranges': 'bytes',
                    'Cache-Control': 'public, max-age=31536000, immutable'
                };
                let status = 200;
                if (object.range) {
                    const { offset, length } = object.range;
                    const end = offset + length - 1;
                    headers['Content-Range'] = `bytes ${offset}-${end}/${object.size}`;
                    headers['Content-Length'] = String(length);
                    status = 206;
                } else if (typeof object.size === 'number') {
                    headers['Content-Length'] = String(object.size);
                }

                if (request.method === 'HEAD') {
                    object.body?.cancel();
                    return new Response(null, { status, headers });
                }
                return new Response(object.body, { status, headers });
            }
        } catch (err) {
            console.error('Song file cache read error:', err);
        }

        let metadata = await readSongCache(context.env.SEARCH_CACHE, cacheKey);
        if (!metadata) {
            metadata = await loadSongMetadata(context.env.SEARCH_CACHE, cacheKey, resolvedSource, id, quality, streamPath);
        }

        if (!metadata || !metadata.sourceUrl) {
            return new Response(JSON.stringify({ code: 404, error: 'Song metadata missing' }), {
                status: 404,
                headers: { 'Content-Type': 'application/json', ...corsHeaders }
            });
        }

        const fetchAudio = async () => {
            const upstreamHeaders = new Headers();
            upstreamHeaders.set('User-Agent', COMMON_HEADERS['User-Agent']);
            if (resolvedSource === 'qq') upstreamHeaders.set('Referer', PLATFORM_REFERERS.qq);
            else if (resolvedSource === 'netease') upstreamHeaders.set('Referer', PLATFORM_REFERERS.netease);
            else if (resolvedSource === 'kuwo') upstreamHeaders.set('Referer', PLATFORM_REFERERS.kuwo);

            const upstreamResp = await fetch(metadata!.sourceUrl as string, {
                headers: upstreamHeaders
            });
            if (!upstreamResp.ok || !upstreamResp.body) {
                throw new Error(`Upstream audio HTTP ${upstreamResp.status}`);
            }
            return upstreamResp;
        };

        let upstreamResp: Response;
        try {
            upstreamResp = await fetchAudio();
        } catch (err) {
            metadata = await loadSongMetadata(context.env.SEARCH_CACHE, cacheKey, resolvedSource, id, quality, streamPath);
            if (!metadata || !metadata.sourceUrl) {
                return new Response(JSON.stringify({ code: 502, error: 'Failed to refresh song metadata' }), {
                    status: 502,
                    headers: { 'Content-Type': 'application/json', ...corsHeaders }
                });
            }
            upstreamResp = await fetchAudio();
        }

        const contentType = upstreamResp.headers.get('Content-Type') || 'audio/mpeg';
        const contentLength = upstreamResp.headers.get('Content-Length');

        if (request.method === 'HEAD') {
            return new Response(null, {
                status: 200,
                headers: {
                    'Content-Type': contentType,
                    'Accept-Ranges': 'bytes',
                    ...(contentLength ? { 'Content-Length': contentLength } : {})
                }
            });
        }

        if (!upstreamResp.body) {
            return new Response(JSON.stringify({ code: 502, error: 'Upstream audio missing body' }), {
                status: 502,
                headers: { 'Content-Type': 'application/json', ...corsHeaders }
            });
        }

        const [clientStream, storageStream] = upstreamResp.body.tee();
        context.waitUntil((async () => {
            try {
                await context.env.SEARCH_CACHE.put(audioKey, storageStream, {
                    httpMetadata: { contentType }
                });
                await writeSongCache(context.env.SEARCH_CACHE, cacheKey, {
                    ...metadata!,
                    url: streamPath,
                    hasFile: true
                });
            } catch (err) {
                console.error('Failed to persist song audio:', err);
            }
        })());

        const headers: Record<string, string> = {
            'Content-Type': contentType,
            'Accept-Ranges': 'bytes',
            'Cache-Control': 'public, max-age=31536000, immutable'
        };
        if (contentLength) {
            headers['Content-Length'] = contentLength;
        }

        return new Response(clientStream, {
            status: 200,
            headers
        });
    }

    if (type === 'lyrics') {
        if (request.method !== 'GET') {
            return new Response('Method not allowed', {
                status: 405,
                headers: { 'Content-Type': 'text/plain', ...corsHeaders }
            });
        }

        if (!source || (source !== 'netease' && source !== 'kuwo' && source !== 'qq' && source !== 'tencent')) {
            return new Response('Invalid source', {
                status: 400,
                headers: { 'Content-Type': 'text/plain', ...corsHeaders }
            });
        }

        const resolvedSource = source === 'tencent' ? 'qq' : source;
        const id = params.id;
        if (!id) {
            return new Response('Missing song id', {
                status: 400,
                headers: { 'Content-Type': 'text/plain', ...corsHeaders }
            });
        }

        const lyricsKey = buildSongLyricsKey(resolvedSource, id);
        const cachedLyrics = await readSongLyrics(context.env.SEARCH_CACHE, lyricsKey);
        if (cachedLyrics) {
            return new Response(cachedLyrics, {
                headers: { 'Content-Type': 'text/plain; charset=utf-8', ...corsHeaders }
            });
        }

        const lrcParam = params.lrc;
        const quality = params.quality || '320k';
        const cacheKey = buildSongCacheKey(resolvedSource, id, quality);
        const streamPath = buildSongStreamPath(resolvedSource, id, quality);

        let lyricSource = lrcParam;
        let metadata: SongCachePayload | null = null;
        if (!lyricSource) {
            metadata = await readSongCache(context.env.SEARCH_CACHE, cacheKey);
            if (!metadata) {
                metadata = await loadSongMetadata(context.env.SEARCH_CACHE, cacheKey, resolvedSource, id, quality, streamPath);
            }
            lyricSource = metadata?.lrc;
        }

        const lyricText = await resolveLyricText(resolvedSource, lyricSource);
        if (lyricText) {
            await writeSongLyrics(context.env.SEARCH_CACHE, lyricsKey, lyricText);
            return new Response(lyricText, {
                headers: { 'Content-Type': 'text/plain; charset=utf-8', ...corsHeaders }
            });
        }

        return new Response('', { headers: { 'Content-Type': 'text/plain; charset=utf-8', ...corsHeaders } });
    }

    if (type === 'cache-search') {
        if (request.method !== 'POST') {
            return new Response(JSON.stringify({ code: 405, error: 'Method not allowed' }), {
                status: 405,
                headers: { 'Content-Type': 'application/json', ...corsHeaders }
            });
        }

        if (!source || (source !== 'netease' && source !== 'kuwo' && source !== 'qq' && source !== 'tencent')) {
            return new Response(JSON.stringify({ code: 400, error: 'Invalid source' }), {
                status: 400,
                headers: { 'Content-Type': 'application/json', ...corsHeaders }
            });
        }

        const resolvedSource = source === 'tencent' ? 'qq' : source;
        let payload: Partial<CachedSearchPayload> | null = null;
        try {
            payload = await request.json();
        } catch (err: any) {
            return new Response(JSON.stringify({ code: 400, error: `Invalid body: ${err?.message || err}` }), {
                status: 400,
                headers: { 'Content-Type': 'application/json', ...corsHeaders }
            });
        }

        const keyword = typeof payload?.keyword === 'string' ? payload.keyword.trim() : '';
        const results = Array.isArray(payload?.results) ? payload?.results : [];
        if (!keyword || !results.length) {
            return new Response(JSON.stringify({ code: 400, error: 'Missing keyword or results' }), {
                status: 400,
                headers: { 'Content-Type': 'application/json', ...corsHeaders }
            });
        }

        const page = Number.isFinite(payload?.page) && (payload?.page as number) > 0
            ? Number(payload?.page)
            : 1;
        const limit = Number.isFinite(payload?.limit) && (payload?.limit as number) > 0
            ? Number(payload?.limit)
            : results.length;
        const total = Number.isFinite(payload?.total) && (payload?.total as number) >= 0
            ? Number(payload?.total)
            : results.length;

        const cacheKey = buildSearchCacheKey(resolvedSource, normalizeSearchKeyword(keyword), page, limit);
        await writeSearchCache(context.env.SEARCH_CACHE, cacheKey, {
            keyword,
            page,
            limit,
            total,
            results
        });

        return new Response(JSON.stringify({ code: 0, cached: true }), {
            headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
    }

    if (type === 'search' && (source === 'netease' || source === 'kuwo' || source === 'qq' || source === 'tencent')) {
        const resolvedSource = source === 'tencent' ? 'qq' : source;
        return handleTunehubSearch(context.env, resolvedSource, params, corsHeaders);
    }

    // ==================== 网易云音乐 ====================
    if (source === 'netease') {
        try {
            if (type === 'toplists') {
                let list: any[] = [];
                try {
                    list = await fetchToplistsViaTunehubApi('netease');
                } catch (err) {
                    console.error('Netease toplists api fallback error:', err);
                }
                if (!list.length) {
                    try {
                        const data = await fetchTunehubMethodData('netease', 'toplists', {});
                        list = (data.list || data.data?.list || []).map((t: any) => normalizeToplistItem(t, 'netease'));
                    } catch (err) {
                        console.error('Netease toplists method error:', err);
                    }
                }
                return new Response(JSON.stringify({ code: 0, data: { list } }), {
                    headers: { 'Content-Type': 'application/json', ...corsHeaders }
                });
            }

            if (type === 'toplist' || type === 'playlist') {
                const { id } = params;
                const method = type === 'toplist' ? 'toplist' : 'playlist';
                let parsedList: any[] = [];
                if (type === 'toplist') {
                    try {
                        parsedList = await fetchToplistSongsViaTunehubApi('netease', id || '', 'netease');
                    } catch (err) {
                        console.error('Netease toplist api fallback error:', err);
                    }
                }

                // --- Netease playlist: use v6 API + song/detail to get ALL tracks ---
                if (type === 'playlist') {
                    try {
                        const neteaseHeaders = {
                            'Referer': 'https://music.163.com/',
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                            'Content-Type': 'application/x-www-form-urlencoded',
                        };

                        // Step 1: Get playlist detail with trackIds via v6 API
                        const detailResp = await fetch('https://music.163.com/api/v6/playlist/detail', {
                            method: 'POST',
                            headers: neteaseHeaders,
                            body: `id=${id}&n=100000&s=8`,
                        });
                        const detailData = await detailResp.json() as any;
                        const playlist = detailData.playlist || detailData.result || {};
                        const trackIds: number[] = (playlist.trackIds || []).map((t: any) => t.id);
                        const partialTracks: any[] = playlist.tracks || [];

                        // Step 2: If we have all tracks already, use them directly
                        if (partialTracks.length >= trackIds.length) {
                            parsedList = partialTracks.map((s: any) => normalizeSongItem(s, 'netease'));
                        } else if (trackIds.length > 0) {
                            // Step 3: Batch fetch song details for remaining track IDs
                            // Use the IDs we already have from partial tracks
                            const existingIds = new Set(partialTracks.map((t: any) => t.id));
                            const missingIds = trackIds.filter(tid => !existingIds.has(tid));

                            // Start with partial tracks we already have
                            parsedList = partialTracks.map((s: any) => normalizeSongItem(s, 'netease'));

                            // Fetch missing tracks in batches of 200
                            const BATCH_SIZE = 200;
                            for (let i = 0; i < missingIds.length; i += BATCH_SIZE) {
                                const batch = missingIds.slice(i, i + BATCH_SIZE);
                                try {
                                    const songResp = await fetch('https://music.163.com/api/song/detail', {
                                        method: 'POST',
                                        headers: neteaseHeaders,
                                        body: `ids=[${batch.join(',')}]`,
                                    });
                                    const songData = await songResp.json() as any;
                                    const songs = songData.songs || [];
                                    parsedList.push(...songs.map((s: any) => normalizeSongItem(s, 'netease')));
                                } catch (batchErr) {
                                    console.error(`Netease song/detail batch error (offset ${i}):`, batchErr);
                                }
                            }

                            // Re-sort to match original trackIds order
                            const idOrder = new Map(trackIds.map((tid, idx) => [String(tid), idx]));
                            parsedList.sort((a, b) => (idOrder.get(a.id) ?? 9999) - (idOrder.get(b.id) ?? 9999));
                        }

                        const info = playlist;
                        return new Response(JSON.stringify({
                            code: 0,
                            data: {
                                info: {
                                    id: String(info.id || id || ''),
                                    name: String(info.name || ''),
                                    desc: String(info.description || info.desc || ''),
                                    author: String(info.creator?.nickname || info.author || ''),
                                    pic: String(info.coverImgUrl || info.pic || ''),
                                    playCount: Number(info.playCount || info.playnum || 0)
                                },
                                list: parsedList
                            }
                        }), { headers: { 'Content-Type': 'application/json', ...corsHeaders } });
                    } catch (err) {
                        console.error('Netease playlist v6 error:', err);
                        // Fall through to method-based approach below
                    }
                }

                if (!parsedList.length) {
                    try {
                        const playlistVars = type === 'playlist'
                            ? { id, limit: 9999, pageSize: 9999 }
                            : { id };
                        const data = await fetchTunehubMethodData('netease', method, playlistVars);
                        const songList = type === 'playlist'
                            ? extractPlaylistTracks(data, 'netease')
                            : extractSearchList(data);
                        parsedList = songList.map((s: any) => normalizeSongItem(s, 'netease'));

                        const info = data.result || data.playlist || data.data?.playlist || {};
                        return new Response(JSON.stringify({
                            code: 0,
                            data: type === 'playlist' ? {
                                info: {
                                    id: String(info.id || id || ''),
                                    name: String(info.name || ''),
                                    desc: String(info.description || info.desc || ''),
                                    author: String(info.creator?.nickname || info.author || ''),
                                    pic: String(info.coverImgUrl || info.pic || ''),
                                    playCount: Number(info.playCount || info.playnum || 0)
                                },
                                list: parsedList
                            } : { source: 'netease', list: parsedList }
                        }), { headers: { 'Content-Type': 'application/json', ...corsHeaders } });
                    } catch (err) {
                        console.error('Netease toplist method error:', err);
                    }
                }

                return new Response(JSON.stringify({
                    code: 0,
                    data: type === 'playlist' ? {
                        info: {
                            id: String(id || ''),
                            name: '',
                            desc: '',
                            author: '',
                            pic: '',
                            playCount: 0
                        },
                        list: parsedList
                    } : { source: 'netease', list: parsedList }
                }), { headers: { 'Content-Type': 'application/json', ...corsHeaders } });
            }
        } catch (e: any) {
            console.error('Netease toplist error:', e);
            return new Response(JSON.stringify({ code: 0, data: { list: [] } }), {
                headers: { 'Content-Type': 'application/json', ...corsHeaders }
            });
        }
    }

    // ==================== 酷我音乐 ====================
    if (source === 'kuwo') {
        try {
            if (type === 'toplists') {
                let list: any[] = [];
                try {
                    list = await fetchToplistsViaTunehubApi('kuwo', 'kuwo');
                } catch (err) {
                    console.error('Kuwo toplists api fallback error:', err);
                }
                if (!list.length) {
                    const data = await fetchTunehubMethodData('kuwo', 'toplists', {});
                    const entries = flattenToplistEntries(data);
                    list = entries.map(item => normalizeToplistItem(item, 'kuwo'));
                }
                return new Response(JSON.stringify({ code: 0, data: { list } }), {
                    headers: { 'Content-Type': 'application/json', ...corsHeaders }
                });
            }

            if (type === 'toplist' || type === 'playlist') {
                const { id } = params;
                const method = type === 'toplist' ? 'toplist' : 'playlist';
                let parsedList: any[] = [];
                if (type === 'toplist') {
                    try {
                        parsedList = await fetchToplistSongsViaTunehubApi('kuwo', id || '', 'kuwo');
                    } catch (err) {
                        console.error('Kuwo toplist api fallback error:', err);
                    }
                }

                if (!parsedList.length) {
                    const kuwoVars = type === 'playlist'
                        ? { id, limit: 9999, pageSize: 9999 }
                        : { id };
                    const data = await fetchTunehubMethodData('kuwo', method, kuwoVars);
                    const songList = type === 'playlist'
                        ? extractPlaylistTracks(data, 'kuwo')
                        : extractSearchList(data);
                    parsedList = songList.map(s => normalizeSongItem(s, 'kuwo'));

                    const info = data.data || data || {};
                    return new Response(JSON.stringify({
                        code: 0,
                        data: type === 'playlist' ? {
                            info: {
                                id: String(info.id || id),
                                name: info.name,
                                desc: info.info || info.desc,
                                pic: info.img || info.pic,
                                playCount: info.playnum
                            },
                            list: parsedList
                        } : { source: 'kuwo', list: parsedList }
                    }), { headers: { 'Content-Type': 'application/json', ...corsHeaders } });
                }

                return new Response(JSON.stringify({
                    code: 0,
                    data: type === 'playlist' ? {
                        info: {
                            id: String(id || ''),
                            name: '',
                            desc: '',
                            pic: '',
                            playCount: 0
                        },
                        list: parsedList
                    } : { source: 'kuwo', list: parsedList }
                }), { headers: { 'Content-Type': 'application/json', ...corsHeaders } });
            }
        } catch (e: any) {
            console.error('Kuwo toplist error:', e);
            return new Response(JSON.stringify({ code: 0, data: { list: [] } }), {
                headers: { 'Content-Type': 'application/json', ...corsHeaders }
            });
        }
    }

    // ==================== QQ音乐 ====================
    if (source === 'qq' || source === 'tencent') {
        try {
            if (type === 'toplists') {
                let list: any[] = [];
                try {
                    list = await fetchToplistsViaTunehubApi('qq', 'qq');
                } catch (err) {
                    console.error('QQ toplists api fallback error:', err);
                }
                if (!list.length) {
                    try {
                        const data = await fetchTunehubMethodData('tencent', 'toplists', {});
                        data.list?.data?.group?.forEach((g: any) => {
                            g.toplist?.forEach((t: any) => {
                                list.push(normalizeToplistItem(t, 'qq'));
                            });
                        });
                        if (list.length === 0 && Array.isArray(data.data?.list)) {
                            data.data.list.forEach((t: any) => list.push(normalizeToplistItem(t, 'qq')));
                        }
                    } catch (err) {
                        console.error('QQ toplists method error:', err);
                    }
                }
                return new Response(JSON.stringify({ code: 0, data: { list } }), {
                    headers: { 'Content-Type': 'application/json', ...corsHeaders }
                });
            }

            if (type === 'toplist' || type === 'playlist') {
                const { id } = params;
                const method = type === 'toplist' ? 'toplist' : 'playlist';
                let parsedList: any[] = [];
                if (type === 'toplist') {
                    try {
                        parsedList = await fetchToplistSongsViaTunehubApi('qq', id || '', 'qq');
                    } catch (err) {
                        console.error('QQ toplist api fallback error:', err);
                    }
                }

                if (!parsedList.length) {
                    try {
                        const qqVars = type === 'playlist'
                            ? { id, limit: 9999, pageSize: 9999 }
                            : { id };
                        const data = await fetchTunehubMethodData('tencent', method, qqVars);
                        const songList = type === 'playlist'
                            ? extractPlaylistTracks(data, 'qq')
                            : extractSearchList(data);
                        parsedList = songList.map((s: any) => normalizeSongItem(s, 'qq'));

                        const info = data.detail?.data || data.cdlist?.[0] || data.data || {};
                        return new Response(JSON.stringify({
                            code: 0,
                            data: type === 'playlist' ? {
                                info: {
                                    id: String(info.disstid || info.id || id),
                                    name: info.dissname || info.name,
                                    desc: info.desc,
                                    author: info.nickname || info.author,
                                    pic: info.logo || info.pic,
                                    playCount: info.visitnum || info.playCount
                                },
                                list: parsedList
                            } : { source: 'qq', list: parsedList }
                        }), { headers: { 'Content-Type': 'application/json', ...corsHeaders } });
                    } catch (err) {
                        console.error('QQ toplist method error:', err);
                    }
                }

                return new Response(JSON.stringify({
                    code: 0,
                    data: type === 'playlist' ? {
                        info: {
                            id: String(id || ''),
                            name: '',
                            desc: '',
                            author: '',
                            pic: '',
                            playCount: 0
                        },
                        list: parsedList
                    } : { source: 'qq', list: parsedList }
                }), { headers: { 'Content-Type': 'application/json', ...corsHeaders } });
            }
        } catch (e: any) {
            return new Response(JSON.stringify({ code: 500, error: `QQ Music error: ${e.message}` }), {
                status: 500,
                headers: { 'Content-Type': 'application/json', ...corsHeaders }
            });
        }
    }

    return new Response('Method not allowed', {
        status: 405,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
};
