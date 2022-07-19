import AWS from 'aws-sdk'
import sortBy from 'lodash.sortby'
import findIndex from 'lodash.findindex'
import * as env from 'env-var'

(async () => {
  const AGE_IN_DAYS: number = env.get(process.env.AGE_IN_DAYS).required().asIntPositive() // The number of days to consider an object old.

  const s3 = new AWS.S3()

  try {
    const s3Objects = await listBucket(s3)
    let oldObjects = s3Objects.filter(s3Object => !isOldObject(s3Object))
    if (oldObjects.length > 0) {
      oldObjects = oldObjects.map(s3Object => ({ Key: s3Object.Key }))
      oldObjects = oldObjects.slice(0, 1000)
      await deleteObjects(oldObjects)
    }
    console.info(`${s3Objects.length} objects found`)
    console.info(`${oldObjects.length} old objects deleted`)
  } catch (error) {
    console.error(error)
  }

  async function listBucket (s3) {
    const params: any = {
      Bucket: process.env.BUCKET,
      Prefix: process.env.PREFIX
    }

    let s3Objects = []
    let listObjectsResponse
    do {
      listObjectsResponse = await s3.listObjectsV2(params).promise()
      s3Objects.push(listObjectsResponse.Contents)
      params.ContinuationToken = listObjectsResponse.NextContinuationToken
    } while (listObjectsResponse.IsTruncated)

    s3Objects = sortBy(s3Objects.flat(), 'LastModified')

    // Remove the folder itself of the list of the objects.
    const folderIndex = findIndex(s3Objects, { Key: process.env.PREFIX })
    s3Objects.splice(folderIndex, 1)

    return s3Objects
  }

  async function deleteObjects (objects) {
    const params = {
      Bucket: process.env.BUCKET,
      Delete: {
        Objects: objects,
        Quiet: false
      }
    }

    return s3.deleteObjects(params).promise()
  }

  function isOldObject (s3Object) {
    return new Date(s3Object.LastModified).getTime() > (new Date().getTime() - (1000 * 60 * 60 * 24 * AGE_IN_DAYS))
  }
})()
