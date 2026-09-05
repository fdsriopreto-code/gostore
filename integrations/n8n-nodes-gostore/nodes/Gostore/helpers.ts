import { createHash, createHmac } from 'crypto';
import * as aws4 from 'aws4';

export interface GostoreCreds {
	endpoint: string;
	region: string;
	accessKeyId: string;
	secretAccessKey: string;
}

export interface SignedRequest {
	url: string;
	headers: Record<string, string>;
}

function base(creds: GostoreCreds): URL {
	return new URL(creds.endpoint.replace(/\/+$/, ''));
}

/** Encode an object key for a path-style URL: encode each segment, keep the slashes. */
export function encodeKey(key: string): string {
	return key
		.split('/')
		.map((s) => encodeURIComponent(s))
		.join('/');
}

/** Build `?a=b&c=d` from a map, skipping undefined/empty values. RFC3986 encoded, key-sorted. */
export function buildQuery(params: Record<string, string | number | undefined>): string {
	const pairs = Object.entries(params)
		.filter(([, v]) => v !== undefined && v !== '' && v !== null)
		.map(([k, v]) => [rfc3986(k), rfc3986(String(v))] as [string, string])
		.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
	return pairs.length ? '?' + pairs.map(([k, v]) => `${k}=${v}`).join('&') : '';
}

function rfc3986(s: string): string {
	return encodeURIComponent(s).replace(
		/[!'()*]/g,
		(c) => '%' + c.charCodeAt(0).toString(16).toUpperCase(),
	);
}

/**
 * Sign a request with SigV4 (Authorization header) using aws4. `path` must
 * already include the query string. Returns the absolute URL and the headers
 * to send (Host is dropped — the HTTP client sets it from the URL).
 */
export function signRequest(
	creds: GostoreCreds,
	method: string,
	path: string,
	body?: Buffer | string,
	extraHeaders?: Record<string, string>,
): SignedRequest {
	const u = base(creds);
	const opts: aws4.Request = {
		host: u.host,
		path,
		service: 's3',
		region: creds.region || 'us-east-1',
		method,
		headers: { ...(extraHeaders ?? {}) },
	};
	if (body !== undefined) opts.body = body;
	aws4.sign(opts, {
		accessKeyId: creds.accessKeyId,
		secretAccessKey: creds.secretAccessKey,
	});
	const headers: Record<string, string> = {};
	for (const [k, v] of Object.entries(opts.headers ?? {})) {
		if (k.toLowerCase() === 'host') continue;
		headers[k] = String(v);
	}
	return { url: `${u.protocol}//${u.host}${path}`, headers };
}

/**
 * Build a presigned URL (query-string auth). `path` must already include any
 * transform/query params to be covered by the signature. gostore signs every
 * query param, so nothing can be appended after this.
 */
export function presignUrl(
	creds: GostoreCreds,
	method: string,
	path: string,
	expiresIn = 3600,
): string {
	const u = base(creds);
	const region = creds.region || 'us-east-1';

	const now = new Date();
	const amzDate = now.toISOString().replace(/[:-]|\.\d{3}/g, '');
	const dateStamp = amzDate.slice(0, 8);

	const qMark = path.indexOf('?');
	const rawPath = qMark === -1 ? path : path.slice(0, qMark);
	const rawQs = qMark === -1 ? '' : path.slice(qMark + 1);

	const canonicalUri = rawPath
		.split('/')
		.map((s) => (s === '' ? '' : encodeURIComponent(s)))
		.join('/');

	const q: Record<string, string> = {};
	if (rawQs) {
		for (const kv of rawQs.split('&')) {
			const [k, v] = kv.split('=');
			q[decodeURIComponent(k)] = decodeURIComponent(v ?? '');
		}
	}
	q['X-Amz-Algorithm'] = 'AWS4-HMAC-SHA256';
	q['X-Amz-Credential'] = `${creds.accessKeyId}/${dateStamp}/${region}/s3/aws4_request`;
	q['X-Amz-Date'] = amzDate;
	q['X-Amz-Expires'] = String(expiresIn);
	q['X-Amz-SignedHeaders'] = 'host';

	const canonicalQs = Object.keys(q)
		.sort()
		.map((k) => `${rfc3986(k)}=${rfc3986(q[k])}`)
		.join('&');

	const canonicalRequest = [
		method,
		canonicalUri,
		canonicalQs,
		`host:${u.host}\n`,
		'host',
		'UNSIGNED-PAYLOAD',
	].join('\n');

	const scope = `${dateStamp}/${region}/s3/aws4_request`;
	const stringToSign = [
		'AWS4-HMAC-SHA256',
		amzDate,
		scope,
		createHash('sha256').update(canonicalRequest).digest('hex'),
	].join('\n');

	const kDate = createHmac('sha256', 'AWS4' + creds.secretAccessKey).update(dateStamp).digest();
	const kRegion = createHmac('sha256', kDate).update(region).digest();
	const kService = createHmac('sha256', kRegion).update('s3').digest();
	const kSigning = createHmac('sha256', kService).update('aws4_request').digest();
	const signature = createHmac('sha256', kSigning).update(stringToSign).digest('hex');

	return `${u.protocol}//${u.host}${canonicalUri}?${canonicalQs}&X-Amz-Signature=${signature}`;
}

/** Turn `X-Gostore-Expire-After` style durations through untouched; validate loosely. */
export function normalizeExpireAfter(v: string): string {
	return v.trim();
}
