import type {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	IDataObject,
} from 'n8n-workflow';
import { NodeOperationError } from 'n8n-workflow';
import { XMLParser } from 'fast-xml-parser';

import {
	type GostoreCreds,
	encodeKey,
	buildQuery,
	signRequest,
	presignUrl,
} from './helpers';

const xml = new XMLParser({ ignoreAttributes: false, parseTagValue: true });

function creds(raw: IDataObject): GostoreCreds {
	return {
		endpoint: String(raw.endpoint ?? '').trim(),
		region: String(raw.region ?? 'us-east-1').trim() || 'us-east-1',
		accessKeyId: String(raw.accessKeyId ?? ''),
		secretAccessKey: String(raw.secretAccessKey ?? ''),
	};
}

export class Gostore implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'gostore',
		name: 'gostore',
		icon: 'file:gostore.svg',
		group: ['output'],
		version: 1,
		subtitle: '={{ $parameter["operation"] + ": " + $parameter["resource"] }}',
		description: 'S3-compatible object storage — objects, buckets, presigned URLs, image transforms, ingest keys, admin',
		defaults: { name: 'gostore' },
		inputs: ['main'],
		outputs: ['main'],
		usableAsTool: true,
		credentials: [{ name: 'gostoreApi', required: true }],
		properties: [
			{
				displayName: 'Resource',
				name: 'resource',
				type: 'options',
				noDataExpression: true,
				options: [
					{ name: 'Object', value: 'object' },
					{ name: 'Bucket', value: 'bucket' },
					{ name: 'Image', value: 'image' },
					{ name: 'Ingest', value: 'ingest' },
					{ name: 'Admin', value: 'admin' },
				],
				default: 'object',
			},

			// ---------------- Object ----------------
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: { show: { resource: ['object'] } },
				options: [
					{ name: 'Upload', value: 'upload', action: 'Upload an object' },
					{ name: 'Download', value: 'download', action: 'Download an object' },
					{ name: 'Delete', value: 'delete', action: 'Delete an object' },
					{ name: 'List', value: 'list', action: 'List objects in a bucket' },
					{ name: 'Copy', value: 'copy', action: 'Copy an object' },
					{ name: 'Get Metadata', value: 'head', action: 'Get object metadata (HEAD)' },
					{ name: 'Get Presigned URL', value: 'presign', action: 'Create a presigned URL' },
				],
				default: 'upload',
			},

			// ---------------- Bucket ----------------
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: { show: { resource: ['bucket'] } },
				options: [
					{ name: 'Create', value: 'create', action: 'Create a bucket' },
					{ name: 'Delete', value: 'delete', action: 'Delete a bucket' },
					{ name: 'List', value: 'list', action: 'List buckets' },
				],
				default: 'list',
			},

			// ---------------- Image ----------------
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: { show: { resource: ['image'] } },
				options: [
					{ name: 'Transform', value: 'transform', action: 'Download a resized/cropped image' },
				],
				default: 'transform',
			},

			// ---------------- Ingest ----------------
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: { show: { resource: ['ingest'] } },
				options: [
					{ name: 'Push', value: 'push', action: 'Push a file with an ingest key (no SigV4)' },
				],
				default: 'push',
			},

			// ---------------- Admin ----------------
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: { show: { resource: ['admin'] } },
				options: [
					{ name: 'Create Snapshot', value: 'snapshot', action: 'Snapshot a bucket' },
					{ name: 'Run Backup Now', value: 'backupRun', action: 'Trigger the scheduled self-backup' },
					{ name: 'Get Folder Usage', value: 'folderUsage', action: 'Storage used by a folder' },
					{ name: 'Get Data Usage', value: 'dataUsage', action: 'Per-bucket object counts and bytes' },
				],
				default: 'folderUsage',
			},

			// ---------------- shared fields ----------------
			{
				displayName: 'Bucket',
				name: 'bucket',
				type: 'string',
				default: '',
				required: true,
				displayOptions: {
					show: {
						resource: ['object', 'bucket', 'image', 'ingest'],
					},
					hide: {
						resource: ['bucket'],
						operation: ['list'],
					},
				},
			},
			{
				displayName: 'Bucket',
				name: 'bucket',
				type: 'string',
				default: '',
				displayOptions: { show: { resource: ['admin'], operation: ['snapshot', 'folderUsage'] } },
				required: true,
			},
			{
				displayName: 'Key',
				name: 'key',
				type: 'string',
				default: '',
				required: true,
				placeholder: 'avatars/123/original.webp',
				displayOptions: {
					show: {
						resource: ['object', 'image'],
						operation: ['upload', 'download', 'delete', 'head', 'presign', 'transform'],
					},
				},
			},

			// upload
			{
				displayName: 'Input Binary Field',
				name: 'binaryPropertyName',
				type: 'string',
				default: 'data',
				required: true,
				displayOptions: { show: { resource: ['object'], operation: ['upload'] } },
				description: 'Name of the binary property on the input item that holds the file',
			},
			{
				displayName: 'Options',
				name: 'uploadOptions',
				type: 'collection',
				placeholder: 'Add option',
				default: {},
				displayOptions: { show: { resource: ['object'], operation: ['upload'] } },
				options: [
					{
						displayName: 'Content Type',
						name: 'contentType',
						type: 'string',
						default: '',
						description: 'Overrides the MIME type from the binary data',
					},
					{
						displayName: 'Expire After',
						name: 'expireAfter',
						type: 'string',
						default: '',
						placeholder: '72h / 7d / 2w',
						description: 'Per-object TTL (X-Gostore-Expire-After) — the object self-deletes',
					},
					{
						displayName: 'Cache-Control',
						name: 'cacheControl',
						type: 'string',
						default: '',
					},
					{
						displayName: 'Metadata (JSON)',
						name: 'metadata',
						type: 'json',
						default: '{}',
						description: 'Object of x-amz-meta-* values, e.g. {"owner":"123"}',
					},
				],
			},

			// download / transform output
			{
				displayName: 'Put Output In Field',
				name: 'outputBinaryField',
				type: 'string',
				default: 'data',
				required: true,
				displayOptions: {
					show: { resource: ['object', 'image'], operation: ['download', 'transform'] },
				},
			},

			// copy
			{
				displayName: 'Source Bucket',
				name: 'sourceBucket',
				type: 'string',
				default: '',
				required: true,
				displayOptions: { show: { resource: ['object'], operation: ['copy'] } },
			},
			{
				displayName: 'Source Key',
				name: 'sourceKey',
				type: 'string',
				default: '',
				required: true,
				displayOptions: { show: { resource: ['object'], operation: ['copy'] } },
			},
			{
				displayName: 'Destination Key',
				name: 'key',
				type: 'string',
				default: '',
				required: true,
				displayOptions: { show: { resource: ['object'], operation: ['copy'] } },
			},

			// list
			{
				displayName: 'Prefix',
				name: 'prefix',
				type: 'string',
				default: '',
				displayOptions: { show: { resource: ['object'], operation: ['list'] } },
			},
			{
				displayName: 'Delimiter',
				name: 'delimiter',
				type: 'string',
				default: '',
				placeholder: '/',
				description: 'Set to "/" to list one folder level (returns CommonPrefixes)',
				displayOptions: { show: { resource: ['object'], operation: ['list'] } },
			},
			{
				displayName: 'Return All',
				name: 'returnAll',
				type: 'boolean',
				default: false,
				displayOptions: { show: { resource: ['object'], operation: ['list'] } },
			},
			{
				displayName: 'Limit',
				name: 'limit',
				type: 'number',
				typeOptions: { minValue: 1 },
				default: 1000,
				displayOptions: {
					show: { resource: ['object'], operation: ['list'], returnAll: [false] },
				},
			},

			// presign / transform options
			{
				displayName: 'HTTP Method',
				name: 'presignMethod',
				type: 'options',
				options: [
					{ name: 'GET (download)', value: 'GET' },
					{ name: 'PUT (upload)', value: 'PUT' },
				],
				default: 'GET',
				displayOptions: { show: { resource: ['object'], operation: ['presign'] } },
			},
			{
				displayName: 'Expires In (seconds)',
				name: 'expiresIn',
				type: 'number',
				default: 3600,
				displayOptions: { show: { resource: ['object'], operation: ['presign'] } },
			},
			{
				displayName: 'Transform',
				name: 'transform',
				type: 'collection',
				placeholder: 'Add parameter',
				default: {},
				displayOptions: { show: { resource: ['image'], operation: ['transform'] } },
				options: [
					{ displayName: 'Width', name: 'w', type: 'number', default: 0 },
					{ displayName: 'Height', name: 'h', type: 'number', default: 0 },
					{
						displayName: 'Fit',
						name: 'fit',
						type: 'options',
						options: [
							{ name: 'Contain', value: 'contain' },
							{ name: 'Cover (crop)', value: 'cover' },
						],
						default: 'contain',
					},
					{
						displayName: 'Format',
						name: 'format',
						type: 'options',
						options: [
							{ name: 'Keep', value: '' },
							{ name: 'JPEG', value: 'jpeg' },
							{ name: 'PNG', value: 'png' },
						],
						default: '',
					},
					{ displayName: 'Quality', name: 'q', type: 'number', default: 0 },
				],
			},

			// ingest
			{
				displayName: 'Ingest Key',
				name: 'ingestKey',
				type: 'string',
				typeOptions: { password: true },
				default: '',
				required: true,
				placeholder: 'gik_...',
				displayOptions: { show: { resource: ['ingest'], operation: ['push'] } },
			},
			{
				displayName: 'Key (path under the ingest prefix)',
				name: 'key',
				type: 'string',
				default: '',
				placeholder: 'pg/dump.sql.gz   (end with / to auto-date)',
				displayOptions: { show: { resource: ['ingest'], operation: ['push'] } },
			},
			{
				displayName: 'Input Binary Field',
				name: 'binaryPropertyName',
				type: 'string',
				default: 'data',
				required: true,
				displayOptions: { show: { resource: ['ingest'], operation: ['push'] } },
			},

			// admin folder usage
			{
				displayName: 'Prefix',
				name: 'prefix',
				type: 'string',
				default: '',
				placeholder: 'projeto-a/',
				displayOptions: { show: { resource: ['admin'], operation: ['folderUsage'] } },
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const out: INodeExecutionData[] = [];
		const raw = (await this.getCredentials('gostoreApi')) as IDataObject;
		const c = creds(raw);

		for (let i = 0; i < items.length; i++) {
			try {
				const resource = this.getNodeParameter('resource', i) as string;
				const operation = this.getNodeParameter('operation', i) as string;
				const result = await runOne.call(this, c, resource, operation, i);
				if (Array.isArray(result)) out.push(...result);
				else if (result) out.push(result);
			} catch (err) {
				if (this.continueOnFail()) {
					out.push({ json: { error: (err as Error).message }, pairedItem: i });
					continue;
				}
				throw err;
			}
		}
		return [out];
	}
}

async function runOne(
	this: IExecuteFunctions,
	c: GostoreCreds,
	resource: string,
	operation: string,
	i: number,
): Promise<INodeExecutionData | INodeExecutionData[] | null> {
	const paired = { item: i };

	// ---------------- Bucket ----------------
	if (resource === 'bucket') {
		if (operation === 'list') {
			const { url, headers } = signRequest(c, 'GET', '/');
			const body = (await this.helpers.httpRequest({ method: 'GET', url, headers })) as string;
			const parsed = xml.parse(body) as IDataObject;
			const lb = (parsed.ListAllMyBucketsResult ?? {}) as IDataObject;
			const bucketsNode = ((lb.Buckets ?? {}) as IDataObject).Bucket;
			const arr = Array.isArray(bucketsNode) ? bucketsNode : bucketsNode ? [bucketsNode] : [];
			return arr.map((b) => ({ json: b as IDataObject, pairedItem: paired }));
		}
		const bucket = this.getNodeParameter('bucket', i) as string;
		const method = operation === 'create' ? 'PUT' : 'DELETE';
		const { url, headers } = signRequest(c, method, `/${encodeURIComponent(bucket)}`);
		await this.helpers.httpRequest({ method, url, headers });
		return { json: { bucket, [operation]: true }, pairedItem: paired };
	}

	// ---------------- Object ----------------
	if (resource === 'object') {
		const bucket = this.getNodeParameter('bucket', i) as string;

		if (operation === 'list') {
			const prefix = this.getNodeParameter('prefix', i, '') as string;
			const delimiter = this.getNodeParameter('delimiter', i, '') as string;
			const returnAll = this.getNodeParameter('returnAll', i, false) as boolean;
			const limit = this.getNodeParameter('limit', i, 1000) as number;

			const objects: IDataObject[] = [];
			const prefixes: string[] = [];
			let token: string | undefined;
			do {
				const q = buildQuery({
					'list-type': 2,
					prefix: prefix || undefined,
					delimiter: delimiter || undefined,
					'continuation-token': token,
					'max-keys': returnAll ? 1000 : Math.min(limit - objects.length, 1000),
				});
				const { url, headers } = signRequest(c, 'GET', `/${encodeURIComponent(bucket)}${q}`);
				const body = (await this.helpers.httpRequest({ method: 'GET', url, headers })) as string;
				const res = ((xml.parse(body) as IDataObject).ListBucketResult ?? {}) as IDataObject;

				const contents = res.Contents;
				const cArr = Array.isArray(contents) ? contents : contents ? [contents] : [];
				for (const o of cArr) objects.push(o as IDataObject);

				const cp = res.CommonPrefixes;
				const cpArr = Array.isArray(cp) ? cp : cp ? [cp] : [];
				for (const p of cpArr) prefixes.push(String((p as IDataObject).Prefix));

				token =
					res.IsTruncated === true || res.IsTruncated === 'true'
						? String(res.NextContinuationToken)
						: undefined;
				if (!returnAll && objects.length >= limit) break;
			} while (token);

			const trimmed = returnAll ? objects : objects.slice(0, limit);
			const rows: INodeExecutionData[] = trimmed.map((o) => ({ json: o, pairedItem: paired }));
			for (const p of prefixes) rows.push({ json: { folder: p }, pairedItem: paired });
			return rows.length ? rows : [{ json: { bucket, objects: 0 }, pairedItem: paired }];
		}

		if (operation === 'upload') {
			const key = this.getNodeParameter('key', i) as string;
			const binProp = this.getNodeParameter('binaryPropertyName', i) as string;
			const opts = this.getNodeParameter('uploadOptions', i, {}) as IDataObject;
			const bin = this.helpers.assertBinaryData(i, binProp);
			const buf = await this.helpers.getBinaryDataBuffer(i, binProp);

			const headers: Record<string, string> = {
				'Content-Type': String(opts.contentType || bin.mimeType || 'application/octet-stream'),
			};
			if (opts.expireAfter) headers['X-Gostore-Expire-After'] = String(opts.expireAfter).trim();
			if (opts.cacheControl) headers['Cache-Control'] = String(opts.cacheControl);
			if (opts.metadata) {
				let md: IDataObject = {};
				try {
					md = typeof opts.metadata === 'string' ? JSON.parse(opts.metadata) : (opts.metadata as IDataObject);
				} catch {
					throw new NodeOperationError(this.getNode(), 'Metadata is not valid JSON');
				}
				for (const [k, v] of Object.entries(md)) headers[`x-amz-meta-${k}`] = String(v);
			}

			const { url, headers: signed } = signRequest(
				c,
				'PUT',
				`/${encodeURIComponent(bucket)}/${encodeKey(key)}`,
				buf,
				headers,
			);
			const resp = (await this.helpers.httpRequest({
				method: 'PUT',
				url,
				headers: signed,
				body: buf,
				returnFullResponse: true,
			})) as { headers: IDataObject };
			return {
				json: {
					bucket,
					key,
					size: buf.length,
					etag: String(resp.headers?.etag ?? '').replace(/"/g, ''),
				},
				pairedItem: paired,
			};
		}

		if (operation === 'download') {
			const key = this.getNodeParameter('key', i) as string;
			const outField = this.getNodeParameter('outputBinaryField', i) as string;
			const { url, headers } = signRequest(
				c,
				'GET',
				`/${encodeURIComponent(bucket)}/${encodeKey(key)}`,
			);
			const resp = (await this.helpers.httpRequest({
				method: 'GET',
				url,
				headers,
				encoding: 'arraybuffer',
				returnFullResponse: true,
			})) as { body: ArrayBuffer | Buffer; headers: IDataObject };
			const buf = Buffer.from(resp.body as ArrayBuffer);
			const fileName = key.split('/').pop() || key;
			const binary = await this.helpers.prepareBinaryData(
				buf,
				fileName,
				String(resp.headers?.['content-type'] ?? 'application/octet-stream'),
			);
			return {
				json: { bucket, key, size: buf.length },
				binary: { [outField]: binary },
				pairedItem: paired,
			};
		}

		if (operation === 'delete') {
			const key = this.getNodeParameter('key', i) as string;
			const { url, headers } = signRequest(
				c,
				'DELETE',
				`/${encodeURIComponent(bucket)}/${encodeKey(key)}`,
			);
			await this.helpers.httpRequest({ method: 'DELETE', url, headers });
			return { json: { bucket, key, deleted: true }, pairedItem: paired };
		}

		if (operation === 'head') {
			const key = this.getNodeParameter('key', i) as string;
			const { url, headers } = signRequest(
				c,
				'HEAD',
				`/${encodeURIComponent(bucket)}/${encodeKey(key)}`,
			);
			const resp = (await this.helpers.httpRequest({
				method: 'HEAD',
				url,
				headers,
				returnFullResponse: true,
			})) as { headers: IDataObject };
			return { json: { bucket, key, headers: resp.headers }, pairedItem: paired };
		}

		if (operation === 'copy') {
			const key = this.getNodeParameter('key', i) as string;
			const srcBucket = this.getNodeParameter('sourceBucket', i) as string;
			const srcKey = this.getNodeParameter('sourceKey', i) as string;
			const { url, headers } = signRequest(
				c,
				'PUT',
				`/${encodeURIComponent(bucket)}/${encodeKey(key)}`,
				undefined,
				{ 'x-amz-copy-source': `/${encodeURIComponent(srcBucket)}/${encodeKey(srcKey)}` },
			);
			const body = (await this.helpers.httpRequest({ method: 'PUT', url, headers })) as string;
			return {
				json: { bucket, key, copiedFrom: `${srcBucket}/${srcKey}`, response: body },
				pairedItem: paired,
			};
		}

		if (operation === 'presign') {
			const key = this.getNodeParameter('key', i) as string;
			const method = this.getNodeParameter('presignMethod', i, 'GET') as string;
			const expiresIn = this.getNodeParameter('expiresIn', i, 3600) as number;
			const link = presignUrl(
				c,
				method,
				`/${encodeURIComponent(bucket)}/${encodeKey(key)}`,
				expiresIn,
			);
			return { json: { bucket, key, method, expiresIn, url: link }, pairedItem: paired };
		}
	}

	// ---------------- Image ----------------
	if (resource === 'image' && operation === 'transform') {
		const bucket = this.getNodeParameter('bucket', i) as string;
		const key = this.getNodeParameter('key', i) as string;
		const outField = this.getNodeParameter('outputBinaryField', i) as string;
		const t = this.getNodeParameter('transform', i, {}) as IDataObject;
		const q = buildQuery({
			w: t.w ? Number(t.w) : undefined,
			h: t.h ? Number(t.h) : undefined,
			fit: (t.fit as string) || undefined,
			format: (t.format as string) || undefined,
			q: t.q ? Number(t.q) : undefined,
		});
		const path = `/${encodeURIComponent(bucket)}/${encodeKey(key)}${q}`;
		const { url, headers } = signRequest(c, 'GET', path);
		const resp = (await this.helpers.httpRequest({
			method: 'GET',
			url,
			headers,
			encoding: 'arraybuffer',
			returnFullResponse: true,
		})) as { body: ArrayBuffer | Buffer; headers: IDataObject };
		const buf = Buffer.from(resp.body as ArrayBuffer);
		const fileName = (key.split('/').pop() || key).replace(/\.[^.]+$/, '') +
			(t.format ? `.${t.format}` : '');
		const binary = await this.helpers.prepareBinaryData(
			buf,
			fileName,
			String(resp.headers?.['content-type'] ?? 'image/jpeg'),
		);
		return {
			json: { bucket, key, size: buf.length, transform: t },
			binary: { [outField]: binary },
			pairedItem: paired,
		};
	}

	// ---------------- Ingest ----------------
	if (resource === 'ingest' && operation === 'push') {
		const bucket = this.getNodeParameter('bucket', i) as string;
		const key = this.getNodeParameter('key', i, '') as string;
		const ingestKey = this.getNodeParameter('ingestKey', i) as string;
		const binProp = this.getNodeParameter('binaryPropertyName', i) as string;
		const bin = this.helpers.assertBinaryData(i, binProp);
		const buf = await this.helpers.getBinaryDataBuffer(i, binProp);
		const endpoint = c.endpoint.replace(/\/+$/, '');
		const url = `${endpoint}/gostore/ingest/${encodeURIComponent(bucket)}/${encodeKey(key)}`;
		const resp = (await this.helpers.httpRequest({
			method: 'PUT',
			url,
			body: buf,
			headers: {
				Authorization: `Bearer ${ingestKey}`,
				'Content-Type': bin.mimeType || 'application/octet-stream',
			},
			json: false,
		})) as string;
		let parsed: IDataObject;
		try {
			parsed = JSON.parse(resp);
		} catch {
			parsed = { response: resp };
		}
		return { json: parsed, pairedItem: paired };
	}

	// ---------------- Admin ----------------
	if (resource === 'admin') {
		const adminGet = async (path: string) => {
			const { url, headers } = signRequest(c, 'GET', `/gostore/admin/v1/${path}`);
			return this.helpers.httpRequest({ method: 'GET', url, headers, json: true });
		};
		const adminPost = async (path: string) => {
			const { url, headers } = signRequest(c, 'POST', `/gostore/admin/v1/${path}`);
			return this.helpers.httpRequest({ method: 'POST', url, headers, json: true });
		};

		if (operation === 'dataUsage') {
			return { json: (await adminGet('datausage')) as IDataObject, pairedItem: paired };
		}
		if (operation === 'folderUsage') {
			const bucket = this.getNodeParameter('bucket', i) as string;
			const prefix = this.getNodeParameter('prefix', i, '') as string;
			const q = buildQuery({ bucket, prefix: prefix || undefined });
			return { json: (await adminGet(`du${q}`)) as IDataObject, pairedItem: paired };
		}
		if (operation === 'snapshot') {
			const bucket = this.getNodeParameter('bucket', i) as string;
			return {
				json: (await adminPost(`snapshot${buildQuery({ bucket })}`)) as IDataObject,
				pairedItem: paired,
			};
		}
		if (operation === 'backupRun') {
			return { json: (await adminPost('backup/run')) as IDataObject, pairedItem: paired };
		}
	}

	throw new NodeOperationError(this.getNode(), `Unsupported ${resource}/${operation}`);
}
