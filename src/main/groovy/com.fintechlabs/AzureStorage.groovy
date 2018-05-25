package com.fintechlabs

import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.blob.*
import groovyx.gpars.GParsExecutorsPool

class AzureStorage {

    static void listSourceContainers(String connectionString) throws Exception {
        CloudStorageAccount storageAccount = CloudStorageAccount.parse(connectionString)
        CloudBlobClient blobClient = storageAccount.createCloudBlobClient()
        blobClient.listContainers().each { CloudBlobContainer blobContainer ->
            blobContainer.listBlobs("", Boolean.TRUE).each { ListBlobItem blobItem ->
                println("URI of blob for Container  ${blobContainer.name} is: " + blobItem.getUri())
            }
            println("\n\n\n")
        }
    }

    static String getOneDayReadToken(CloudBlobContainer blobContainer) throws Exception {
        SharedAccessBlobPolicy blobPolicy = new SharedAccessBlobPolicy()
        Calendar calendar = Calendar.getInstance()
        calendar.setTime(new Date())
        blobPolicy.setSharedAccessStartTime(calendar.getTime())
        calendar.set(Calendar.HOUR_OF_DAY, 24)
        blobPolicy.setSharedAccessExpiryTime(calendar.getTime())
        blobPolicy.setPermissions(EnumSet.of(SharedAccessBlobPermissions.READ))
        return blobContainer.generateSharedAccessSignature(blobPolicy, null)
    }

    static void copyContainer(String srcConnectionString, String destConnectionString) throws Exception {
        CloudStorageAccount srcStorageAccount = CloudStorageAccount.parse(srcConnectionString)
        CloudBlobClient srcBlobClient = srcStorageAccount.createCloudBlobClient()
        List<CloudBlobContainer> sourceContainerList = []
        srcBlobClient.listContainers().each { CloudBlobContainer blobContainer -> sourceContainerList << blobContainer }

        CloudStorageAccount destStorageAccount = CloudStorageAccount.parse(destConnectionString)
        CloudBlobClient destBlobClient = destStorageAccount.createCloudBlobClient()
        List<CloudBlobContainer> destContainerList = []
        destBlobClient.listContainers("fin360-").each { CloudBlobContainer blobContainer -> destContainerList << blobContainer }

        GParsExecutorsPool.withPool(sourceContainerList.size() + 1) {
            sourceContainerList.eachWithIndexParallel { CloudBlobContainer sourceContainer, Integer index ->
                CloudBlobContainer destContainer = destContainerList.get(index)
                println("Source Container       ========>>>>>>      ${sourceContainer?.getName()}")
                println("Destination Container Exist  ${destContainer.getName()}   ======>>>>>     " + destContainer.exists())
                copyBlob(sourceContainer, destContainerList.get(index))
            }
        }
    }

    static void copyBlob(CloudBlobContainer sourceBlobContainer, CloudBlobContainer destBlobContainer) throws Exception {
        String srcBlobToken = getOneDayReadToken(sourceBlobContainer)
        println("Token      ====>>>>>>      ${srcBlobToken}")
        sourceBlobContainer.listBlobs("", Boolean.TRUE).each { ListBlobItem blobItem ->
            println("Transfering data from ${sourceBlobContainer.getName()} to ${destBlobContainer.getName()}   **********************")
            CloudBlob sourceBlob = sourceBlobContainer.getBlockBlobReference(blobItem.getUri().toString())
            CloudBlob destBlob
            String[] uriStrArr = sourceBlob.getName().split(sourceBlobContainer.getName() + "/")
            if (sourceBlob.getProperties().getBlobType() == BlobType.BLOCK_BLOB) {
                destBlob = destBlobContainer.getBlockBlobReference(uriStrArr[1])
            } else {
                destBlob = destBlobContainer.getPageBlobReference(uriStrArr[1])
            }
            println("URI   ${destBlobContainer.getName()}  =========>>>>>>>        " + (sourceBlob.getName() + "?" + srcBlobToken))
            destBlob.startCopy(new URI(sourceBlob.getName() + "?" + srcBlobToken))
        }
        println("*****************      Data Transfer Successful       *****************")
    }

}
