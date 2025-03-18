import path from 'path';
import slugify from 'slugify';
import { Storage } from '@google-cloud/storage';
import { pipeline } from 'stream/promises';

/**
 * A native implementation of _.get.
 */
const get = (obj, pathStr, defaultValue = undefined) => {
  const travel = (regexp) =>
    String.prototype.split.call(pathStr, regexp)
      .filter(Boolean)
      .reduce((res, key) => (res !== null && res !== undefined ? res[key] : res), obj);
  const result = travel(/[,[\]]+?/) || travel(/[,[\].]+?/);
  return result === undefined || result === obj ? defaultValue : result;
};

/**
 * Sets a configuration field value.
 */
const setConfigField = (fieldValue, defaultValue) => {
  if (typeof defaultValue === 'undefined') throw new Error('Default value is required!');
  if (typeof fieldValue === 'undefined') return defaultValue;
  switch (typeof fieldValue) {
    case 'boolean':
      return fieldValue;
    case 'string':
      if (['true', 'false'].includes(fieldValue)) return fieldValue === 'true';
      throw new Error(`Invalid boolean value for ${fieldValue}!`);
    default:
      return defaultValue;
  }
};

/**
 * Check validity of Service Account configuration.
 */
const checkServiceAccount = (config = {}) => {
  if (!config.bucketName) {
    throw new Error('"Bucket name" is required!');
  }
  if (!config.baseUrl) {
    config.baseUrl = 'https://storage.googleapis.com/{bucket-name}';
  }
  if (!config.basePath) {
    config.basePath = '';
  }
  config.publicFiles = setConfigField(config.publicFiles, true);
  config.uniform = setConfigField(config.uniform, false);
  config.skipCheckBucket = setConfigField(config.skipCheckBucket, false);

  let serviceAccount;
  if (config.serviceAccount) {
    try {
      serviceAccount = typeof config.serviceAccount === 'string'
        ? JSON.parse(config.serviceAccount)
        : config.serviceAccount;
      // Replace escaped newline characters in private_key
      if (serviceAccount.private_key) {
        serviceAccount.private_key = serviceAccount.private_key.replace(/\\n/g, '\n');
      }
    } catch (e) {
      throw new Error(
        'Error parsing "Service Account JSON". Please ensure you copy the full JSON file.'
      );
    }
    if (!serviceAccount.project_id) {
      throw new Error('Missing "project_id" field in Service Account JSON.');
    }
    if (!serviceAccount.client_email) {
      throw new Error('Missing "client_email" field in Service Account JSON.');
    }
    if (!serviceAccount.private_key) {
      throw new Error('Missing "private_key" field in Service Account JSON.');
    }
  }
  return serviceAccount;
};

/**
 * Check if bucket exists.
 */
const checkBucket = async (GCS, bucketName) => {
  const bucket = GCS.bucket(bucketName);
  const [exists] = await bucket.exists();
  if (!exists) {
    throw new Error(`Bucket "${bucketName}" does not exist on Google Cloud Storage.`);
  }
};

/**
 * Merge provider config with any additional custom configurations.
 * (Updated to remove the dependency on a global "strapi" variable.)
 */
const mergeConfigs = (providerConfig) => {
  // For the latest Strapi versions, assume providerConfig is complete.
  return providerConfig;
};

/**
 * Generate an upload file name including path.
 */
const generateUploadFileName = (basePath, file) => {
  const backupPath = file.related && file.related.length > 0 && file.related[0].ref
    ? `${file.related[0].ref}`
    : `${file.hash}`;
  const filePath = file.path ? `${file.path}/` : `${backupPath}/`;
  const extension = file.ext.toLowerCase();
  const fileName = slugify(path.basename(file.hash), { lower: true, strict: true });
  return `${basePath}${filePath}${fileName}${extension}`;
};

/**
 * Prepare file before upload.
 */
const prepareUploadFile = async (file, config, basePath, GCS) => {
  let deleteFile = false;
  const fullFileName =
    typeof config.generateUploadFileName === 'function'
      ? await config.generateUploadFileName(file)
      : generateUploadFileName(basePath, file);
  if (!config.skipCheckBucket) {
    await checkBucket(GCS, config.bucketName);
  }
  const bucket = GCS.bucket(config.bucketName);
  const bucketFile = bucket.file(fullFileName);
  const [fileExists] = await bucketFile.exists();
  if (fileExists) {
    deleteFile = true;
  }
  const asciiFileName = file.name.normalize('NFKD').replace(/[\u0300-\u036f]/g, '');
  const fileAttributes = {
    contentType: typeof config.getContentType === 'function' ? config.getContentType(file) : file.mime,
    gzip: typeof config.gzip === 'boolean' ? config.gzip : 'auto',
    metadata: typeof config.metadata === 'function'
      ? config.metadata(file)
      : {
          contentDisposition: `inline; filename="${asciiFileName}"`,
          cacheControl: `public, max-age=${config.cacheMaxAge || 3600}`,
        },
  };
  if (!config.uniform) {
    fileAttributes.public = config.publicFiles;
  }
  return { fileAttributes, bucketFile, fullFileName, deleteFile };
};

/**
 * Initialize the Google Cloud Storage provider.
 */
const init = (providerConfig) => {
  const config = mergeConfigs(providerConfig);
  const serviceAccount = checkServiceAccount(config);
  let GCS;
  if (serviceAccount) {
    // Provide service account credentials.
    GCS = new Storage({
      projectId: serviceAccount.project_id,
      credentials: {
        client_email: serviceAccount.client_email,
        private_key: serviceAccount.private_key,
      },
    });
  } else {
    // Fall back to Application Default Credentials.
    GCS = new Storage();
  }
  const basePath = `${config.basePath}/`.replace(/^\/+/, '');
  const baseUrl = config.baseUrl.replace('{bucket-name}', config.bucketName);

  return {
    async upload(file) {
      try {
        const { fileAttributes, bucketFile, fullFileName, deleteFile } = await prepareUploadFile(
          file,
          config,
          basePath,
          GCS
        );
        if (deleteFile) {
          console.info('File already exists. Removing it.');
          await this.delete(file);
        }
        await bucketFile.save(file.buffer, fileAttributes);
        file.url = `${baseUrl}/${fullFileName}`;
        console.debug(`File successfully uploaded to ${file.url}`);
      } catch (error) {
        console.error(`Error uploading file: ${error.message}`);
        throw error;
      }
    },
    async uploadStream(file) {
      try {
        const { fileAttributes, bucketFile, fullFileName, deleteFile } = await prepareUploadFile(
          file,
          config,
          basePath,
          GCS
        );
        if (deleteFile) {
          console.info('File already exists. Removing it.');
          await this.delete(file);
        }
        await pipeline(file.stream, bucketFile.createWriteStream(fileAttributes));
        file.url = `${baseUrl}/${fullFileName}`;
        console.debug(`File successfully uploaded to ${file.url}`);
      } catch (error) {
        console.error(`Error uploading stream: ${error.message}`);
        throw error;
      }
    },
    async delete(file) {
      if (!file.url) {
        console.warn('No remote file URL found; manual deletion may be required.');
        return;
      }
      const fileName = file.url.replace(`${baseUrl}/`, '');
      const bucket = GCS.bucket(config.bucketName);
      try {
        await bucket.file(fileName).delete();
        console.debug(`File ${fileName} successfully deleted`);
      } catch (error) {
        if (error.code === 404) {
          console.warn('File not found on remote storage; manual deletion may be needed.');
        }
      }
    },
    isPrivate() {
      return !config.publicFiles;
    },
    async getSignedUrl(file) {
      const options = {
        version: 'v4',
        action: 'read',
        expires: config.expires || Date.now() + 15 * 60 * 1000, // 15 minutes from now
      };
      const fileName = file.url.replace(`${baseUrl}/`, '');
      const [url] = await GCS.bucket(config.bucketName).file(fileName).getSignedUrl(options);
      return { url };
    },
  };
};

export {
  get,
  setConfigField,
  checkServiceAccount,
  checkBucket,
  mergeConfigs,
  generateUploadFileName,
  prepareUploadFile,
  init,
};
