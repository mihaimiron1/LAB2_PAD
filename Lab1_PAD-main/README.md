# Agent de mesagerie — Partea 2 (gRPC)

Sistemul este format din trei procese independente: `SenderApp`, `BrokerApp` si `ReceiverApp` (C# / .NET 10, interfata Avalonia).
Senderul nu cunoaste adresa receiverului; comunica numai cu brokerul, iar brokerul ruteaza dupa `topic`.

Arhitectura din Partea 1 ramane aceeasi; se schimba implementarea comunicarii: in locul socket-urilor TCP cu JSON pe linie
se folosesc **apeluri RPC gRPC**, iar datele sunt descrise in **Protocol Buffers** (`.proto`).

## Ce s-a schimbat fata de Partea 1

| | Partea 1 | Partea 2 |
|---|---|---|
| Comunicare | socket-uri TCP, cate un obiect JSON pe linie | gRPC peste HTTP/2 (tot peste TCP) |
| Reprezentarea datelor | JSON, clase copiate in fiecare aplicatie | Protocol Buffers: un singur contract `Protos/messagehub.proto`, clasele C# se genereaza la build (Grpc.Tools) |
| Publicare | actiunea `PUBLISH` | `Publish` (apel unar) |
| Lista topicurilor | `LIST_TOPICS` | `ListTopics` (apel unar) |
| Abonare + confirmare | `SUBSCRIBE`, apoi linii `ACK:<id>` | `Subscribe` (stream bidirectional): primul cadru `SubscribeRequest`, apoi cate un `Ack` |
| Topic suplimentar | `ADD_TOPIC` pe o conexiune TCP noua | `AddTopics` (apel unar) pe acelasi canal HTTP/2 |
| Erori | `{"success":false,"code":...}` | coduri de status gRPC; codul din Partea 1 ramane la inceputul mesajului |

## Contractul

`Protos/messagehub.proto` este inclus prin link in toate proiectele (ca `EnvironmentSettings.cs`): brokerul genereaza partea de server,
senderul si receiverul partea de client.

```proto
service MessageBroker {
  rpc Publish    (PublishRequest)       returns (BrokerReply);         // sender
  rpc ListTopics (ListTopicsRequest)    returns (TopicList);           // sender
  rpc Subscribe  (stream ReceiverFrame) returns (stream BrokerFrame);  // receiver: abonare + ACK <-> livrare
  rpc AddTopics  (AddTopicsRequest)     returns (BrokerReply);         // receiver
}

message Message       { string id = 1; string topic = 2; string payload = 3;
                        google.protobuf.Timestamp timestamp = 4; string sender_id = 5; }
message ReceiverFrame { oneof frame { SubscribeRequest subscribe = 1; Ack ack = 2; } }
message BrokerFrame   { oneof frame { BrokerReply reply = 1; Message message = 2; } }
message BrokerReply   { string code = 1; string detail = 2; string client_endpoint = 3; }
```

Senderul serializeaza mesajul (codul generat il scrie in format binar Protobuf). Brokerul il deserializeaza, citeste tipul mesajului
(`topic`) chiar din corpul lui si il ruteaza. Mesajul ajunge la receiver nealterat: acelasi `id`, `payload`, `timestamp` si `sender_id`.

## Protocol de transport si canale

gRPC foloseste **HTTP/2 peste TCP**, pe portul `BROKER_PORT` din `.env`. TCP ramane alegerea din Partea 1: transmitere fiabila si
ordonata, cu detectarea erorilor de conexiune; peste UDP ar fi trebuit implementate ordonarea, retransmiterea si detectarea pierderilor.
HTTP/2 adauga multiplexarea: mai multe apeluri simultane pe aceeasi conexiune TCP.

- Fiecare client are **un singur canal gRPC = o conexiune HTTP/2** catre broker. Numarul canalelor este variabil: unul pentru fiecare client conectat.
- Senderul trimite pe canalul lui apelurile `Publish` si `ListTopics` (la fiecare 2 secunde).
- Receiverul tine deschis un **stream bidirectional** `Subscribe`: doua semicanale unidirectionale (broker → receiver pentru mesaje,
  receiver → broker pentru ACK-uri). `AddTopics` circula pe aceeasi conexiune.
- Numarul de canale logice (stream-uri HTTP/2) pe o conexiune se stabileste la crearea serverului: `MaxStreamsPerConnection = 100`
  (`BrokerServer.cs`). Un client foloseste cel mult cateva simultan (receiverul: stream-ul `Subscribe` + `AddTopics`;
  senderul: `Publish` + `ListTopics`), deci o singura conexiune per client este suficienta si pastreaza ordinea mesajelor.
- Topicurile sunt canalele logice dintre producatori si consumatori.

## Identificarea participantilor

- **Senderul** are un nume logic, `ID SENDER` (`sender_id` in fiecare mesaj). Brokerul il valideaza si il trece in jurnal,
  iar receiverul il afiseaza la fiecare mesaj („de la sender_1”).
- **Receiverul** are un `ClientId` unic printre receiverii conectati (`CLIENT_ID_TAKEN` la duplicat). Cat timp receiverul este conectat,
  un `AddTopics` pentru ClientId-ul lui este acceptat doar pe conexiunea lui (altfel `PermissionDenied`, `NOT_OWNER`).
  Verificarea compara adresa IP:port a conexiunii; un clic pe „Adauga” exact in momentul unei reconectari poate fi refuzat o data.
  Pentru un abonat deconectat ramane regula din Partea 1: topicurile i se pot adauga (din interfata, butonul „Adauga” este activ doar cat receiverul e conectat).
- Fiecare terminal primeste de la sistemul de operare un port local liber. Brokerul trimite in fiecare raspuns adresa `IP:port`
  de pe care vede clientul (`client_endpoint`); aceasta apare in ferestrele senderului si receiverului, iar brokerul afiseaza adresa fiecarui abonat.

## Rutare si model de comunicare

Rutarea se face dupa `topic`. In forma de baza, un sender si un receiver pe acelasi topic comunica unu-la-unu.
Implementarea include si extensia Publisher/Subscriber: acelasi mesaj este pus in coada fiecarui receiver abonat la topic (unu-la-mai-multi).

- La abonare, brokerul trimite receiverului istoricul mesajelor topicurilor alese (si la `AddTopics`, pentru topicurile noi).
- Un mesaj publicat pe un topic fara abonati este acceptat si pastrat in istoric (contorul „FARA ABONAT”).
  Il primesc toti receiverii care se aboneaza ulterior la acel topic.

## Validarea datelor

Validarea se face in broker (`BrokerApp/Services/MessageValidator.cs`). Senderul verifica doar adresa brokerului, ca regulile brokerului
sa poata fi vazute direct din interfata.

- **sender_id / ClientId**: 1–32 caractere `A-Z a-z 0-9 _ -`.
- **topic**:
  - 1–64 caractere, fara virgule (virgula separa mai multe topicuri) si fara caractere de control;
  - la `Publish` nu poate incepe sau se termina cu spatii; in `Subscribe`/`AddTopics` spatiile de la capete se elimina.
- **id**: GUID valid; un id deja publicat este respins (`DUPLICATE_MESSAGE`).
- **payload**: nevid, maxim 4096 caractere.
- **timestamp**: obligatoriu, in intervalul valid al tipului `Timestamp` (anii 0001–9999).
- **cerere**: cel putin un topic, maxim 20; un mesaj gRPC poate avea maxim 64 KB.

| Situatie | Cod in Partea 1 | Status gRPC (mesaj) |
|---|---|---|
| Pachet gol / JSON invalid | `EMPTY_PACKET`, `INVALID_JSON` | datele binare invalide sunt respinse la deserializare: la apelurile unare de framework (`Unknown`, fara linie in jurnal), iar pe stream-ul `Subscribe` de broker (`InvalidArgument`, trecut in jurnal) |
| Actiune necunoscuta | `UNKNOWN_ACTION` | `Unimplemented` (metoda nu exista in contract) |
| Mesaj invalid | `INVALID_MESSAGE` | `InvalidArgument` (`INVALID_MESSAGE: ...`) |
| Topic invalid | `INVALID_PACKET` | `InvalidArgument` (`INVALID_TOPIC: ...`) |
| ClientId invalid | `INVALID_SUBSCRIPTION` | `InvalidArgument` (`INVALID_SUBSCRIPTION: ...`) |
| Primul cadru `Subscribe` nu este abonarea | — | `FailedPrecondition` |
| ClientId conectat deja | `CLIENT_ID_TAKEN` | `AlreadyExists` |
| Topic deja abonat | `ALREADY_SUBSCRIBED` | `AlreadyExists` |
| Id de mesaj publicat deja | — | `AlreadyExists` (`DUPLICATE_MESSAGE: ...`) |
| `AddTopics` fara abonare | `UNKNOWN_CLIENT` | `NotFound` |
| `AddTopics` de pe alta conexiune decat a receiverului | — | `PermissionDenied` (`NOT_OWNER: ...`) |
| Mesaj mai mare de 64 KB | — | `ResourceExhausted` |
| Broker oprit | — | `Unavailable` (`BROKER_STOPPED: ...`) |
| Abonamente sterse din broker („Sterge toti”) | — | `Aborted` (`SUBSCRIPTIONS_CLEARED: ...`) |
| Eroare neprevazuta | — | `Internal`; brokerul ramane activ |

Un interceptor gRPC (`BrokerErrorInterceptor`) trece in jurnalul brokerului, ca `ERROR`, cererile respinse de broker (validare,
ClientId ocupat, Id duplicat etc.). Tot el transforma exceptiile neprevazute in `Internal`, ca o cerere gresita sa nu poata opri brokerul.
Respingerile facute de framework inainte ca cererea sa ajunga la broker (ex. un `Publish` peste 64 KB) nu apar in jurnal;
clientul primeste totusi statusul corect.

**Demo de validare:**
- **Din interfata:** in Sender, publicati pe topicul `a,b`, cu mesajul gol sau cu `ID SENDER` = `sender 1` (cu spatiu).
  Motivul dat de broker apare sub buton, iar respingerea apare in jurnalul brokerului.
- **Cu Postman sau grpcurl:** brokerul expune *server reflection*, deci serviciul poate fi apelat direct. In Postman: New → gRPC → `localhost:5000` → *Using server reflection*. Cu grpcurl:

  ```powershell
  grpcurl -plaintext localhost:5000 list
  grpcurl -plaintext -d '{\"message\":{\"id\":\"abc\",\"topic\":\"Curs\",\"payload\":\"x\"}}' localhost:5000 messagehub.MessageBroker/Publish
  ```

  Al doilea apel intoarce `InvalidArgument: INVALID_MESSAGE: Id-ul mesajului trebuie sa fie un GUID valid.`

## Politici de livrare

- **Confirmare**: dupa fiecare mesaj, brokerul asteapta pe stream un `Ack` cu acelasi `id`, timp de 5 secunde. Un mesaj confirmat iese din coada.
- **ACK lipsa (receiver conectat, dar fara confirmare)**: se numara o incercare, mesajul ramane pending, iar work job-ul (care ruleaza la 2 secunde)
  il retrimite; in jurnal apare `WORKER`, iar incercarile sunt la 5–7 secunde una de alta (timeout-ul de 5 s plus urmatorul tick).
  Dupa 5 incercari esuate mesajul ajunge in **dead-letter** (aproximativ 25–35 de secunde).
  Livrarea catre un receiver este in ordine (FIFO): cat timp primul mesaj asteapta ACK, urmatoarele raman in coada („IN ASTEPTARE”).
  *Diferenta fata de Partea 1:* acolo receiverul era declarat offline de la primul ACK lipsa; acum ramane conectat, iar reincercarile continua pe acelasi stream.
  *Demo:* bifati in Receiver „Test: nu trimite ACK” si publicati un mesaj.
- **ACK intarziat sau necunoscut**: este ignorat si notat in jurnal (`ACK`).
- **Receiver deconectat sau cazut**: sesiunea devine OFFLINE; daca o livrare era in curs, se numara o incercare.
  Incercarile se pastreaza si dupa reconectare, deci un mesaj care face receiverul sa cada de 5 ori ajunge in dead-letter
  (nu se repeta la nesfarsit). Mesajele raman pending pana la reconectare. Ping-urile HTTP/2 (15 s, timeout 10 s) detecteaza si receiverii care dispar fara sa inchida conexiunea.
- **Livrare at-least-once**: un receiver poate primi un mesaj de doua ori (dupa un ACK lipsa sau la reconectare).
  Il confirma din nou, iar in lista il marcheaza „duplicat”.
- **Reconectare**: la o noua abonare, brokerul goleste coada receiverului si retrimite istoricul topicurilor abonate,
  cu exceptia mesajelor aflate in dead-letter sau sterse de acolo pentru acel receiver.
- **Dead-letter**:
  - „Retrimite” pune mesajul inapoi in coada, doar daca receiverul mai este abonat; daca receiverul este offline,
    mesajul ii este livrat la prima reconectare (chiar daca intre timp si-a schimbat topicurile). Daca si acea sesiune
    cade inainte de ACK, mesajul revine prin replay doar daca receiverul este inca abonat la topicul lui;
  - „Sterge toti” muta inapoi in dead-letter mesajele retrimise care asteptau reconectarea receiverului;
  - „Sterge” il elimina definitiv: nu mai este trimis nici la reconectare. „Goleste tot” face acelasi lucru pentru toate;
  - „Test esec livrare” simuleaza o incercare esuata pentru primul mesaj pending al unui abonat.
- **„Sterge toti”**: inchide stream-urile tuturor receiverilor cu `Aborted` (acestia nu se reconecteaza automat) si sterge abonamentele,
  cozile, istoricul si mesajele fara abonat. Dead-letter-ul ramane.
- **Inchiderea unui stream de catre broker**: brokerul asteapta terminarea scrierii in curs (maxim 2 s), apoi trimite statusul final
  (`Aborted` / `Unavailable`). Daca ACK-ul receiverului nu mai poate pleca, receiverul citeste statusul final de la broker.
  Doar un receiver care nu mai citeste deloc (scriere blocata peste 2 s) primeste o intrerupere fara status.
- **Mesaje invalide**: sunt respinse cu un status gRPC; brokerul ramane activ.
- **Caderea brokerului**:
  - Stocarea este tranzienta (doar in memorie, fara baza de date si fara fisier), deci mesajele nelivrate se pierd.
  - Senderul primeste `Unavailable`, iar mesajul apare cu ❌ in istoric. Textul ramane in caseta: un nou clic il publica din nou, cu un Id nou.
  - Receiverii care au fost abonati se reconecteaza automat la fiecare 3 secunde, cu acelasi ClientId si aceleasi topicuri, si isi refac abonarea cand brokerul revine.
  - „Opreste” din fereastra brokerului este o oprire controlata: receiverii primesc `Unavailable`, iar starea se sterge.

## Concurenta si stocare

- **Tratarea concurenta a cererilor**: Kestrel (ASP.NET Core) trateaza fiecare apel RPC pe un task separat din thread pool.
  I/O-ul este asincron, deci niciun apel nu blocheaza canalul. Este echivalentul „thread per request”, fara un fir dedicat fiecarei conexiuni.
- **Stocare tranzienta**: `ConcurrentDictionary`, `ConcurrentQueue` si `ConcurrentDictionary` ca set pentru deduplicare. Listele de istoric sunt protejate de `lock`.
- **Livrarea**:
  - Un `SemaphoreSlim` per receiver asigura o singura livrare simultana catre acelasi receiver.
  - Fiecare stream are un lock de scriere, fiindca gRPC permite un singur `WriteAsync` simultan. Acest lock garanteaza si ca raspunsul `SUBSCRIBED` este mereu primul cadru.
  - ACK-ul asteptat este un `TaskCompletionSource`, completat de bucla care citeste stream-ul receiverului.
- **Schimbarile de abonamente** (`Subscribe`, `AddTopics`, „Retrimite”, stergere, oprire) sunt serializate cu un lock; publicarea si livrarea raman concurente.
  O sesiune noua devine vizibila livrarilor abia dupa ce coada ei este pregatita.
- **Publicari vs. golirea starii**: publicarile simultane iau lock-ul de citire al unui `ReaderWriterLockSlim`. „Sterge toti”, oprirea
  si abonarea (pe durata resetarii cozii si a replay-ului) iau lock-ul de scriere. Astfel un mesaj nu poate fi rutat pe jumatate
  catre o stare tocmai stearsa sau resetata; in schimb, publicarile asteapta cateva milisecunde cat dureaza un replay.
- **Set de work joburi (cron)**: 3 joburi periodice, la 2 secunde, pornite decalat. Fiecare raspunde de o partitie a receiverilor
  (hash pe ClientId) si porneste concurent cate o livrare pentru fiecare receiver conectat din partitia lui (cea a unui receiver fara
  mesaje in asteptare se termina imediat); receiverii serviti deja de alta livrare sunt sariti. In jurnal apare `WORKER` cu numarul jobului.
  Prima livrare a unui mesaj nou porneste imediat, la publicare; joburile retrimit periodic ce a ramas in asteptare.
- **Interfete**: jurnalul brokerului si lista receiverului se actualizeaza in loturi, deci o rafala de mii de mesaje nu blocheaza ferestrele.

## Responsabilitatile brokerului si limitari asumate

Corespondenta cu tabelul de responsabilitati din enunt:

| Responsabilitate (enunt) | Implementare |
|---|---|
| Primirea mesajelor | `Publish` (sender): validare, Id unic, istoric |
| Determinarea destinatarilor si rutarea | dupa `topic`, catre fiecare abonat al topicului (cate o coada pe receiver) |
| Tratarea diferentelor dintre interfete | senderul foloseste cereri simple cu raspuns (`Publish`, `ListTopics`), fara stream si fara ACK; receiverul foloseste un stream bidirectional cu confirmari. Brokerul face puntea: pastreaza mesajul, il impinge pe stream, asteapta ACK-ul si il retrimite; senderul nu cunoaste receiverii si nici protocolul lor. Prin *server reflection*, brokerul poate fi apelat si de clienti generici (Postman, grpcurl), fara cod generat. |
| Transmiterea mesajelor | push pe stream-ul `Subscribe`, cu ACK, reincercari si dead-letter |

Limitari asumate:

- **Metoda persistenta** din enunt a fost omisa intentionat, la cererea profesorului (observatie la Partea 1): mesajele nu se salveaza
  in baza de date sau in fisier, iar stocarea este doar tranzienta (colectii concurente in memorie).
- **Inregistrarea participantilor**: receiverii se inregistreaza prin `Subscribe` (ClientId unic, protejat de `NOT_OWNER`). Senderii nu tin
  stare la broker, pentru decuplare: sunt identificati la fiecare mesaj prin `sender_id` (validat) si adresa IP:port.
- **Punct unic de esec**: exista un singur broker. Caderea lui este tratata prin reconectarea automata a receiverilor si prin eroare
  explicita la sender. Dublarea brokerului (primar / secundar cu sincronizarea starii), mentionata in enunt ca remediere posibila,
  nu face parte din implementare.
- **Memorie**: istoricul topicurilor si Id-urile publicate cresc cat timp ruleaza brokerul; se golesc la „Sterge toti” sau la oprire.

## Structura

```
Protos/messagehub.proto          contractul comun
EnvironmentSettings.cs           configurarea din .env (comun tuturor proiectelor)
BrokerChannelFactory.cs          canalul gRPC al clientilor (Sender + Receiver)
BrokerApp/BrokerServer.cs        rutare, stocare tranzienta, livrare, work job, pornirea serverului gRPC
BrokerApp/SubscriberSession.cs   un receiver conectat: scrierile pe stream si ACK-ul asteptat
BrokerApp/Services/              endpoint-ul gRPC, validarea, interceptorul de erori
SenderApp/, ReceiverApp/         clientii
MessageHub.slnx                  solutia cu cele trei proiecte
```

## Rulare

Necesar: .NET 10 SDK. Pentru rulare fara SDK sunt necesare .NET Runtime 10 si, pentru broker, ASP.NET Core Runtime 10.
Configurarea comuna se afla in `.env`:

```env
BROKER_HOST=127.0.0.1
BROKER_PORT=5000
```

Pentru configurarea initiala, copiati `.env.example` in `.env` si modificati valorile daca este necesar. Fisierul `.env` este local si nu se publica in repository.

```powershell
dotnet build MessageHub.slnx
dotnet run --project BrokerApp
dotnet run --project ReceiverApp
dotnet run --project SenderApp
```

Brokerul porneste automat la deschiderea ferestrei. Porniti brokerul, apoi unul sau mai multi receiveri (`Curs`, sau mai multe topicuri
separate prin virgula: `Curs, PAD`), apoi senderul. Un receiver se poate abona si mai tarziu: mesajele publicate intre timp ii sunt livrate la abonare.

## Rulare pe doua calculatoare (retea locala)

Brokerul asculta pe toate interfetele de retea, deci accepta conexiuni si de pe alte calculatoare.

1. Pe calculatorul A porniti `BrokerApp`. In bara de sus apare `Adresa pentru alte calculatoare: 192.168.x.y:5000`.
2. Pe calculatorul A permiteti conexiunile pe portul brokerului in Windows Firewall (o singura data, din PowerShell pornit ca Administrator):

   ```powershell
   New-NetFirewallRule -DisplayName "MessageHub Broker" -Direction Inbound -Protocol TCP -LocalPort 5000 -Action Allow
   ```

   Regula este pe port, deci functioneaza indiferent de calea executabilului.
3. Pe calculatorul B porniti `SenderApp` / `ReceiverApp` si scrieti in campul **ADRESA BROKER** adresa afisata de broker,
   de ex. `192.168.1.10:5000` (se accepta si `http://192.168.1.10:5000`).

Ambele calculatoare trebuie sa fie in aceeasi retea, marcata ca *Private* in Windows. Verificare rapida de pe calculatorul B:
`Test-NetConnection 192.168.1.10 -Port 5000` trebuie sa afiseze `TcpTestSucceeded : True`.

Daca pe calculatorul B nu este instalat .NET 10, aplicatia se poate publica impreuna cu runtime-ul:

```powershell
dotnet publish ReceiverApp -c Release -r win-x64 --self-contained true
```

Unele retele Wi-Fi publice (de ex. ale facultatii) izoleaza dispozitivele intre ele. In acest caz folositi un hotspot de pe telefon.
